#ifdef USE_PRAGMA_IMPLEMENTATION
#pragma implementation        // gcc: Class implementation
#endif

#include "ha_vogal.h"

#define MYSQL_SERVER 1

static handler *vogal_create_handler(handlerton *hton,
                                       TABLE_SHARE *table, 
                                       MEM_ROOT *mem_root);
handlerton *vogal_hton;
static HASH vogal_open_tables;
pthread_mutex_t vogal_mutex;

static uchar* vogal_get_key(VOGAL_SHARE *share, size_t *length,
                             my_bool not_used __attribute__((unused)))
{
	DBUG_ENTER("vogal_get_key");
	*length=share->table_name_length;
	DBUG_RETURN( (uchar*) share->table_name );
}


static int vogal_init_func(void *p)
{
	DBUG_ENTER("vogal_init_func");

	vogal_hton= (handlerton *)p;
	VOID(pthread_mutex_init(&vogal_mutex,MY_MUTEX_INIT_FAST));
	(void) hash_init(&vogal_open_tables,system_charset_info,32,0,0,(hash_get_key) vogal_get_key,0,0);

	vogal_hton->state=   SHOW_OPTION_YES;
	vogal_hton->create=  vogal_create_handler;
	vogal_hton->flags=   HTON_CAN_RECREATE;

	vogal = new vogal_handler();

	DBUG_RETURN(0);
}


static int vogal_done_func(void *p)
{
	DBUG_ENTER("vogal_done_func");
	int error= 0;

	if (vogal_open_tables.records)
		error= 1;
	hash_free(&vogal_open_tables);
	pthread_mutex_destroy(&vogal_mutex);

	if (vogal)
		vogal->~vogal_handler();

	DBUG_RETURN(0);
}

static VOGAL_SHARE *get_share(const char *table_name, TABLE *table)
{
	DBUG_ENTER("get_share");
	
  VOGAL_SHARE *share;
  uint length;
  char *tmp_name;

  pthread_mutex_lock(&vogal_mutex);
  length=(uint) strlen(table_name);

  if (!(share=(VOGAL_SHARE*) hash_search(&vogal_open_tables,
                                           (uchar*) table_name,
                                           length)))
  {
    if (!(share=(VOGAL_SHARE *)
          my_multi_malloc(MYF(MY_WME | MY_ZEROFILL),
                          &share, sizeof(*share),
                          &tmp_name, length+1,
                          NullS)))
    {
      pthread_mutex_unlock(&vogal_mutex);
      DBUG_RETURN(NULL);
    }

    share->use_count=0;
    share->table_name_length=length;
    share->table_name=tmp_name;

	share->cursor = vogal->getManipulation()->openCursor(vogal->getDefinition()->openTable(share->table_name));
	if (!share->cursor) {
		ERROR("Erro ao abrir a tabela!");
		goto error;
	}
	share->filter = NULL;

    strmov(share->table_name,table_name);
    if (my_hash_insert(&vogal_open_tables, (uchar*) share))
      goto error;
    thr_lock_init(&share->lock);
    pthread_mutex_init(&share->mutex,MY_MUTEX_INIT_FAST);
  }
  share->use_count++;
  pthread_mutex_unlock(&vogal_mutex);

  DBUG_RETURN(share);

error:
  pthread_mutex_destroy(&share->mutex);
  my_free(share, MYF(0));

  DBUG_RETURN(NULL);
}

static int free_share(VOGAL_SHARE *share)
{
	DBUG_ENTER("free_share");

	pthread_mutex_lock(&vogal_mutex);
	if (!--share->use_count) {
		hash_delete(&vogal_open_tables, (uchar*) share);
		thr_lock_delete(&share->lock);
		pthread_mutex_destroy(&share->mutex);
		if (share->cursor)
			share->cursor->~CursorType();
		if (share->filter)
			share->filter->~FilterCursorType();
		my_free(share, MYF(0));
	}
	pthread_mutex_unlock(&vogal_mutex);

	DBUG_RETURN(0);
}

static handler* vogal_create_handler(handlerton *hton,
                                       TABLE_SHARE *table, 
                                       MEM_ROOT *mem_root)
{
	DBUG_ENTER("vogal_create_handler");
	DBUG_RETURN( new (mem_root) ha_vogal(hton, table) );
}

ha_vogal::ha_vogal(handlerton *hton, TABLE_SHARE *table_arg) : handler(hton, table_arg) {
	DBUG_ENTER("ha_vogal::ha_vogal");
	DBUG_LEAVE;
}

ha_vogal::~ha_vogal() {
	DBUG_ENTER("ha_vogal::~ha_vogal");
	DBUG_LEAVE;
}
	
const char *ha_vogal::table_type() const { 
	DBUG_ENTER("ha_vogal::table_type");
	DBUG_RETURN(VOGAL);
}

ulonglong ha_vogal::table_flags() const {
	DBUG_ENTER("ha_vogal::table_flags");
	DBUG_RETURN(HA_NO_TRANSACTIONS | HA_REC_NOT_IN_SEQ | HA_NO_AUTO_INCREMENT | HA_BINLOG_ROW_CAPABLE);
}

uint ha_vogal::max_supported_record_length() const { 
	DBUG_ENTER("ha_vogal::max_supported_record_length");
	DBUG_RETURN(1024); // 1KB (definição do projeto)
}

// TODO: Refinar estatística
double ha_vogal::scan_time() { 
	DBUG_ENTER("ha_vogal::scan_time");
	DBUG_RETURN( (double) (stats.records+stats.deleted) / 20.0+10 );
}

static const char *ha_vogal_exts[] = { VOGAL_EXT, NullS };

const char **ha_vogal::bas_ext() const
{
	DBUG_ENTER("ha_vogal::bas_ext");
	DBUG_RETURN(ha_vogal_exts);
}

int ha_vogal::open(const char *name, int mode, uint test_if_locked)
{
	DBUG_ENTER("ha_vogal::open");

	if (!(share = get_share(name, table)))
		DBUG_RETURN(1); // Lock
	thr_lock_data_init(&share->lock,&lock,NULL);

	DBUG_RETURN(0);
}

int ha_vogal::close(void)
{
  DBUG_ENTER("ha_vogal::close");
  DBUG_RETURN(free_share(share));
}

ulong ha_vogal::index_flags(uint inx, uint part, bool all_parts) const { 
	DBUG_ENTER("ha_vogal::index_flags");
	DBUG_RETURN(0);
}

/*
const char *ha_vogal::index_type(uint inx) {
	DBUG_ENTER("ha_vogal::index_type");
	DBUG_RETURN("HASH");
}

  /** @brief
    unireg.cc will call this to make sure that the storage engine can handle
    the data it is about to send. Return *real* limits of your storage engine
    here; MySQL will do min(your_limits, MySQL_limits) automatically.

      @details
    There is no need to implement ..._key_... methods if your engine doesn't
    support indexes.
uint ha_vogal::max_supported_keys() const {
	DBUG_ENTER("ha_vogal::max_supported_keys");
	DBUG_RETURN(0);
}

  /** @brief
    unireg.cc will call this to make sure that the storage engine can handle
    the data it is about to send. Return *real* limits of your storage engine
    here; MySQL will do min(your_limits, MySQL_limits) automatically.

      @details
    There is no need to implement ..._key_... methods if your engine doesn't
    support indexes.
uint ha_vogal::max_supported_key_parts() const {
	DBUG_ENTER("ha_vogal::max_supported_key_parts");
	DBUG_RETURN(0);
}

uint ha_vogal::max_supported_key_length() const {
	DBUG_ENTER("ha_vogal::max_supported_key_length");
	DBUG_RETURN(0);
}

// TODO: Refinar estatística
virtual double ha_vogal::read_time(ha_rows rows) {
	DBUG_ENTER("ha_vogal::read_time");
	DBUG_RETURN( (double) rows /  20.0+1 );
}

  /** @brief
    We implement this in ha_example.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
int ha_vogal::index_read_map(uchar *buf, const uchar *key, key_part_map keypart_map, enum ha_rkey_function find_flag) {
	DBUG_ENTER("ha_vogal::index_read_map");
	DBUG_RETURN(0);
}

  /** @brief
    We implement this in ha_example.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
int ha_vogal::index_next(uchar *buf) {
	DBUG_ENTER("ha_vogal::index_next");
	DBUG_RETURN(0);
}

  /** @brief
    We implement this in ha_example.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
int ha_vogal::index_prev(uchar *buf) {
	DBUG_ENTER("ha_vogal::index_prev");
	DBUG_RETURN(0);
}

  /** @brief
    We implement this in ha_example.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
int ha_vogal::index_first(uchar *buf) {
	DBUG_ENTER("ha_vogal::index_first");
	DBUG_RETURN(0);
}

  /** @brief
    We implement this in ha_example.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
int ha_vogal::index_last(uchar *buf) {
	DBUG_ENTER("ha_vogal::index_last");
	DBUG_RETURN(0);
}
*/

int ha_vogal::write_row(uchar *buf) {
	DBUG_ENTER("ha_vogal::write_row");

	int error = 0;
	BigNumber * number;
	DataCursorType * data;
	BigNumber rid;
	CursorType * cursor = NULL;
	ValueListRoot * dataList = vlNew(true);
	char* buffer;
	String str_aux;

	// Habilita campos para leitura
	my_bitmap_map *org_bitmap= dbug_tmp_use_all_columns(table, table->read_set);

	// Inicia criação dos dados da tabela
	// Obs: Está sendo considerado que a ordem das colunas no MySQL é a mesma que no Vogal
	for (Field ** field = table->field; *field; field++) {
		if (!bitmap_is_set(table->read_set, (*field)->field_index))
			continue;
		data = new DataCursorType();
		data->column = vogal->getDefinition()->findColumn(share->cursor->table, const_cast<char*>((*field)->field_name));
		if (!data->column) {
			ERROR("Coluna da tabela não pode ser localizada para ler linha!");
			data->~DataCursorType();
			error = HA_ERR_INTERNAL_ERROR;
			goto error;
		}
		switch (data->column->type) {
			case VARCHAR:
				data->contentOwner = false;
				data->usedSize = (*field)->data_length();
				data->allocSize = 1024;
				buffer = new char[data->allocSize];
				str_aux = String(buffer, data->allocSize, NULL);
				(*field)->val_str(&str_aux);
				data->content = (GenericPointer) str_aux.ptr();
				break;
			case NUMBER:
				number = new BigNumber();
				(*number) = (*field)->val_int();
				data->contentOwner = true;
				data->allocSize = sizeof(BigNumber);
				data->usedSize = data->allocSize;
				data->content = (GenericPointer) number;
				break;
			default:
				ERROR("Não preparado para tipo!");
				error = HA_ERR_INTERNAL_ERROR;
				goto error;
		}
		vlAdd(dataList, data);
	}

	rid = vogal->getManipulation()->insertData(share->cursor, dataList);
	if (!rid) {
		ERROR("Erro ao inserir estrutura de dados da tabela OBJS");
		goto error;
	}

error:
	vlFree(&dataList);

	dbug_tmp_restore_column_map(table->read_set, org_bitmap);
  
	DBUG_RETURN(error);
}

// ################
int ha_vogal::update_row(const uchar *old_data, uchar *new_data)
{

  DBUG_ENTER("ha_vogal::update_row");
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}

// ################
int ha_vogal::delete_row(const uchar *buf)
{
  DBUG_ENTER("ha_vogal::delete_row");
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}

int ha_vogal::rnd_init(bool scan) {
	DBUG_ENTER("ha_vogal::rnd_init");

	// Se o cursor estiver aberto, reinicia-o do início
	if (share->filter) {
		share->filter->reset();
		DBUG_RETURN(0);
	}

	share->filter = new FilterCursorType();
	share->filter->cursor = share->cursor;
	share->filter->cursorOwner = false;

	DBUG_RETURN(0);

error:
	if (share->filter) {
		share->filter->~FilterCursorType();
		share->filter = NULL;
	}
	DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
}

int ha_vogal::rnd_end()
{
	DBUG_ENTER("ha_vogal::rnd_end");
	if (share->filter) {
		share->filter->~FilterCursorType();
		share->filter = NULL;
	}
	DBUG_RETURN(0);
}

int ha_vogal::vogal2mysql(CursorType * cursor, RidCursorType * rid) {
	DBUG_ENTER("ha_vogal::vogal2mysql");

	int ret = false;
	DataCursorType * data;
	
	// Habilita campos para escrita
	my_bitmap_map *org_bitmap= dbug_tmp_use_all_columns(table, table->write_set);

	// Obs: Está sendo considerado que a ordem das colunas no MySQL é a mesma que no Vogal
	for (Field ** field = table->field; *field; field++) {
		if (!bitmap_is_set(table->write_set, (*field)->field_index))
			continue;
		data = (DataCursorType*) vlGet(rid->dataList, (*field)->field_index);
		if (!data) {
			ERROR("Dado da coluna da tabela não pode ser localizada para ler linha!");
			goto error;
		}
		switch (data->column->type) {
			case VARCHAR:
				(*field)->store( (char*) data->content, data->usedSize, NULL);
				break;
			case NUMBER:
				(*field)->store( (*(BigNumber*) data->content) );
				break;
			default:
				ERROR("Não preparado para tipo!");
				goto error;
		}
	}

	dbug_tmp_restore_column_map(table->write_set, org_bitmap);

	ret = true;

error:
	DBUG_RETURN(ret);
}

int ha_vogal::rnd_next(uchar *buf) {
	DBUG_ENTER("ha_vogal::rnd_next");

	if (!vogal->getManipulation()->fetch(share->filter)) {
		ERROR("Erro ao efetuar o fetch!");
		goto error;
	}
	
	if (!share->filter->fetch)
		DBUG_RETURN(HA_ERR_END_OF_FILE);

	if (!vogal2mysql(share->filter->cursor, share->filter->fetch)) {
		ERROR("Erro ao atualizar a saída para o banco!");
		goto error;
	}

	DBUG_RETURN(0);
	
error:
	DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
	
}

void ha_vogal::position(const uchar *record)
{
  DBUG_ENTER("ha_vogal::position");
  my_store_ptr(ref, ref_length, share->filter->fetch->id);
  DBUG_VOID_RETURN;
}

int ha_vogal::rnd_pos(uchar *buf, uchar *pos) {
	DBUG_ENTER("ha_vogal::rnd_pos");

	int error = 0;

	RidCursorType * rid = NULL;
	SearchInfoType * info = NULL;

	rid = new RidCursorType();
	rid->id = my_get_ptr(pos, ref_length);

	info = vogal->getManipulation()->findNearest(share->filter->cursor, rid, NULL, share->filter->cursor->table->block);
	if (!info || info->comparison) {
		ERROR("Erro durante a busca do registro na determinada posição!");
		error = HA_ERR_KEY_NOT_FOUND;
		goto error;
	}
	
	if (!vogal2mysql(share->filter->cursor, info->findedNode->rid)) {
		ERROR("Erro ao atualizar a saída para o banco!");
		error = HA_ERR_INTERNAL_ERROR;
		goto error;
	}

error:
	if (rid)
		rid->~RidCursorType();
	if (info)
		info->~SearchInfoType();
	
	DBUG_RETURN(error);
	
}

int ha_vogal::info(uint flag)
{
  DBUG_ENTER("ha_vogal::info");
  DBUG_RETURN(0);
}

int ha_vogal::external_lock(THD *thd, int lock_type)
{
  DBUG_ENTER("ha_vogal::external_lock");
  DBUG_RETURN(0);
}


THR_LOCK_DATA **ha_vogal::store_lock(THD *thd,
                                       THR_LOCK_DATA **to,
                                       enum thr_lock_type lock_type)
{
	DBUG_ENTER("ha_vogal::store_lock");
	
  if (lock_type != TL_IGNORE && lock.type == TL_UNLOCK)
    lock.type=lock_type;
  *to++= &lock;
	
	DBUG_RETURN(to);
}

int ha_vogal::delete_table(const char *name)
{
	DBUG_ENTER("ha_vogal::delete_table");

	if (!vogal->getDefinition()->dropTable(const_cast<char*>(name))) {
		my_error(ER_NO, MYF(0), "Erro ao remover a tabela");
		DBUG_RETURN(HA_ERR_GENERIC);
	}

	DBUG_RETURN(0);
}

int ha_vogal::create(const char *name, TABLE *table_arg,
                       HA_CREATE_INFO *create_info)
{
	DBUG_ENTER("ha_vogal::create");

	PairListRoot * colList = plNew(false, false);
	for (Field **field= table_arg->s->field; *field; field++) {
		char * field_type = NULL;
		switch (vogal_utils::myType2VoType((*field)->type())) {
			case VARCHAR: field_type = C_TYPE_VARCHAR; break;
			case NUMBER: field_type = C_TYPE_NUMBER; break;
		}
		if (!field_type) {
			my_error(ER_CHECK_NOT_IMPLEMENTED, MYF(0), "Tipo desconhecido!");
			DBUG_RETURN(HA_ERR_UNSUPPORTED);
		}
		plAdd(colList, const_cast<char*>( (*field)->field_name ), field_type );
	}
	int created = vogal->getDefinition()->newTable(const_cast<char*>(name), colList);
	if (!created)
		my_error(ER_CANT_CREATE_TABLE, MYF(0), "Erro ao criar a tabela");
	plFree(&colList);
	if (!created)
		DBUG_RETURN(HA_ERR_GENERIC);
  
	DBUG_RETURN(0);
}

struct st_mysql_storage_engine vogal_storage_engine =
{ MYSQL_HANDLERTON_INTERFACE_VERSION };

mysql_declare_plugin(vogal)
{
  MYSQL_STORAGE_ENGINE_PLUGIN,
  &vogal_storage_engine,
  VOGAL,
  "João Paulo Poffo",
  "AEIOU - Armazenamento e Extração de Informações Otimizada por Unicidade",
  PLUGIN_LICENSE_GPL,
  vogal_init_func,                            /* Plugin Init */
  vogal_done_func,                            /* Plugin Deinit */
  0x0001, 				      				  /* 0.1 */
  NULL,                                       /* status variables */
  NULL,                      				  /* system variables */
  NULL                                        /* config options */
}
mysql_declare_plugin_end;
