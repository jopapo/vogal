#ifdef USE_PRAGMA_IMPLEMENTATION
#pragma implementation        // gcc: Class implementation
#endif

#include "ha_vogal.h"

#define MYSQL_SERVER 1

static handler *vogal_create_handler(handlerton *hton,
                                       TABLE_SHARE *table, 
                                       MEM_ROOT *mem_root);

handlerton *vogal_hton;

/* Variables for vogal share methods */

/* 
   Hash used to track the number of open tables; variable for vogal share
   methods
*/
static HASH vogal_open_tables;

/* The mutex used to init the hash; variable for vogal share methods */
pthread_mutex_t vogal_mutex;

/**
  @brief
  Function we use in the creation of our hash to get key.
*/

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
  (void) hash_init(&vogal_open_tables,system_charset_info,32,0,0,
                   (hash_get_key) vogal_get_key,0,0);

  vogal_hton->state=   SHOW_OPTION_YES;
  vogal_hton->create=  vogal_create_handler;
  vogal_hton->flags=   HTON_CAN_RECREATE;

	vogal = new vogal_handler();

  DBUG_RETURN(0);
}


static int vogal_done_func(void *p)
{
  int error= 0;
  DBUG_ENTER("vogal_done_func");

  if (vogal_open_tables.records)
    error= 1;
  hash_free(&vogal_open_tables);
  pthread_mutex_destroy(&vogal_mutex);
  
	if (vogal)
		vogal->~vogal_handler();

  DBUG_RETURN(0);
}


/**
  @brief
  vogal of simple lock controls. The "share" it creates is a
  structure we will pass to each vogal handler. Do you have to have
  one of these? Well, you have pieces that are used for locking, and
  they are needed to function.
*/

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


/**
  @brief
  Free lock controls. We call this whenever we close a table. If the table had
  the last reference to the share, then we free memory associated with it.
*/

static int free_share(VOGAL_SHARE *share)
{
	DBUG_ENTER("free_share");
	
  pthread_mutex_lock(&vogal_mutex);
  if (!--share->use_count)
  {
    hash_delete(&vogal_open_tables, (uchar*) share);
    thr_lock_delete(&share->lock);
    pthread_mutex_destroy(&share->mutex);
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
	DBUG_RETURN(
			HA_NO_TRANSACTIONS | HA_REC_NOT_IN_SEQ | HA_NO_AUTO_INCREMENT |
            HA_BINLOG_ROW_CAPABLE | HA_BINLOG_STMT_CAPABLE);
}

ulong ha_vogal::index_flags(uint inx, uint part, bool all_parts) const { 
	DBUG_ENTER("ha_vogal::index_flags");
	DBUG_RETURN(0);
}

uint ha_vogal::max_supported_record_length() const { 
	DBUG_ENTER("ha_vogal::max_supported_record_length");
	DBUG_RETURN(1024); // 1KB (definição do projeto)
}

double ha_vogal::scan_time() { 
	DBUG_ENTER("ha_vogal::scan_time");
	DBUG_RETURN( (double) (stats.records+stats.deleted) / 20.0+10 );
}

/**
  @brief
  If frm_error() is called then we will use this to determine
  the file extensions that exist for the storage engine. This is also
  used by the default rename_table and delete_table method in
  handler.cc.

  For engines that have two file name extentions (separate meta/index file
  and data file), the order of elements is relevant. First element of engine
  file name extentions array should be meta/index file extention. Second
  element - data file extention. This order is assumed by
  prepare_for_repair() when REPAIR TABLE ... USE_FRM is issued.

  @see
  rename_table method in handler.cc and
  delete_table method in handler.cc
*/

static const char *ha_vogal_exts[] = { VOGAL_EXT, NullS };

const char **ha_vogal::bas_ext() const
{
	DBUG_ENTER("ha_vogal::bas_ext");
	DBUG_RETURN(ha_vogal_exts);
}


/**
  @brief
  Used for opening tables. The name will be the name of the file.

  @details
  A table is opened when it needs to be opened; e.g. when a request comes in
  for a SELECT on the table (tables are not open and closed for each request,
  they are cached).

  Called from handler.cc by handler::ha_open(). The server opens all tables by
  calling ha_open() which then calls the handler specific open().

  @see
  handler::ha_open() in handler.cc
*/

int ha_vogal::open(const char *name, int mode, uint test_if_locked)
{
  DBUG_ENTER("ha_vogal::open");

  if (!(share = get_share(name, table)))
    DBUG_RETURN(1);
  thr_lock_data_init(&share->lock,&lock,NULL);

  DBUG_RETURN(0);
}


/**
  @brief
  Closes a table. We call the free_share() function to free any resources
  that we have allocated in the "shared" structure.

  @details
  Called from sql_base.cc, sql_select.cc, and table.cc. In sql_select.cc it is
  only used to close up temporary tables or during the process where a
  temporary table is converted over to being a myisam table.

  For sql_base.cc look at close_data_tables().

  @see
  sql_base.cc, sql_select.cc and table.cc
*/

int ha_vogal::close(void)
{
  DBUG_ENTER("ha_vogal::close");
  DBUG_RETURN(free_share(share));
}


/**
  @brief
  write_row() inserts a row. No extra() hint is given currently if a bulk load
  is happening. buf() is a byte array of data. You can use the field
  information to extract the data from the native byte array type.

    @details
  vogal of this would be:
    @code
  for (Field **field=table->field ; *field ; field++)
  {
    ...
  }
    @endcode

  See ha_tina.cc for an vogal of extracting all of the data as strings.
  ha_berekly.cc has an vogal of how to store it intact by "packing" it
  for ha_berkeley's own native storage type.

  See the note for update_row() on auto_increments and timestamps. This
  case also applies to write_row().

  Called from item_sum.cc, item_sum.cc, sql_acl.cc, sql_insert.cc,
  sql_insert.cc, sql_select.cc, sql_table.cc, sql_udf.cc, and sql_update.cc.

    @see
  item_sum.cc, item_sum.cc, sql_acl.cc, sql_insert.cc,
  sql_insert.cc, sql_select.cc, sql_table.cc, sql_udf.cc and sql_update.cc
*/

int ha_vogal::write_row(uchar *buf)
{
  DBUG_ENTER("ha_vogal::write_row");
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}


/**
  @brief
  Yes, update_row() does what you expect, it updates a row. old_data will have
  the previous row record in it, while new_data will have the newest data in it.
  Keep in mind that the server can do updates based on ordering if an ORDER BY
  clause was used. Consecutive ordering is not guaranteed.

    @details
  Currently new_data will not have an updated auto_increament record, or
  and updated timestamp field. You can do these for vogal by doing:
    @code
  if (table->timestamp_field_type & TIMESTAMP_AUTO_SET_ON_UPDATE)
    table->timestamp_field->set_time();
  if (table->next_number_field && record == table->record[0])
    update_auto_increment();
    @endcode

  Called from sql_select.cc, sql_acl.cc, sql_update.cc, and sql_insert.cc.

    @see
  sql_select.cc, sql_acl.cc, sql_update.cc and sql_insert.cc
*/
int ha_vogal::update_row(const uchar *old_data, uchar *new_data)
{

  DBUG_ENTER("ha_vogal::update_row");
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}


/**
  @brief
  This will delete a row. buf will contain a copy of the row to be deleted.
  The server will call this right after the current row has been called (from
  either a previous rnd_nexT() or index call).

  @details
  If you keep a pointer to the last row or can access a primary key it will
  make doing the deletion quite a bit easier. Keep in mind that the server does
  not guarantee consecutive deletions. ORDER BY clauses can be used.

  Called in sql_acl.cc and sql_udf.cc to manage internal table
  information.  Called in sql_delete.cc, sql_insert.cc, and
  sql_select.cc. In sql_select it is used for removing duplicates
  while in insert it is used for REPLACE calls.

  @see
  sql_acl.cc, sql_udf.cc, sql_delete.cc, sql_insert.cc and sql_select.cc
*/

int ha_vogal::delete_row(const uchar *buf)
{
  DBUG_ENTER("ha_vogal::delete_row");
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}


/**
  @brief
  rnd_init() is called when the system wants the storage engine to do a table
  scan. See the vogal in the introduction at the top of this file to see when
  rnd_init() is called.
    Unlike index_init(), rnd_init() can be called two consecutive times
    without rnd_end() in between (it only makes sense if scan=1). In this
    case, the second call should prepare for the new table scan (e.g if
    rnd_init() allocates the cursor, the second call should position the
    cursor to the start of the table; no need to deallocate and allocate
    it again. This is a required method.

    @details
  Called from filesort.cc, records.cc, sql_handler.cc, sql_select.cc, sql_table.cc,
  and sql_update.cc.

    @see
  filesort.cc, records.cc, sql_handler.cc, sql_select.cc, sql_table.cc and sql_update.cc
*/
int ha_vogal::rnd_init(bool scan)
{
  DBUG_ENTER("ha_vogal::rnd_init");
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}

int ha_vogal::rnd_end()
{
  DBUG_ENTER("ha_vogal::rnd_end");
  DBUG_RETURN(0);
}


/**
  @brief
  This is called for each row of the table scan. When you run out of records
  you should return HA_ERR_END_OF_FILE. Fill buff up with the row information.
  The Field structure for the table is the key to getting data into buf
  in a manner that will allow the server to understand it.

    @details
  Called from filesort.cc, records.cc, sql_handler.cc, sql_select.cc, sql_table.cc,
  and sql_update.cc.

    @see
  filesort.cc, records.cc, sql_handler.cc, sql_select.cc, sql_table.cc and sql_update.cc
*/
int ha_vogal::rnd_next(uchar *buf)
{
  DBUG_ENTER("ha_vogal::rnd_next");
  DBUG_RETURN(HA_ERR_END_OF_FILE);
}


/**
  @brief
  position() is called after each call to rnd_next() if the data needs
  to be ordered. You can do something like the following to store
  the position:
    @code
  my_store_ptr(ref, ref_length, current_position);
    @endcode

    @details
  The server uses ref to store data. ref_length in the above case is
  the size needed to store current_position. ref is just a byte array
  that the server will maintain. If you are using offsets to mark rows, then
  current_position should be the offset. If it is a primary key like in
  BDB, then it needs to be a primary key.

  Called from filesort.cc, sql_select.cc, sql_delete.cc, and sql_update.cc.

    @see
  filesort.cc, sql_select.cc, sql_delete.cc and sql_update.cc
*/
void ha_vogal::position(const uchar *record)
{
  DBUG_ENTER("ha_vogal::position");
  DBUG_VOID_RETURN;
}


/**
  @brief
  This is like rnd_next, but you are given a position to use
  to determine the row. The position will be of the type that you stored in
  ref. You can use ha_get_ptr(pos,ref_length) to retrieve whatever key
  or position you saved when position() was called.

    @details
  Called from filesort.cc, records.cc, sql_insert.cc, sql_select.cc, and sql_update.cc.

    @see
  filesort.cc, records.cc, sql_insert.cc, sql_select.cc and sql_update.cc
*/
int ha_vogal::rnd_pos(uchar *buf, uchar *pos)
{
  DBUG_ENTER("ha_vogal::rnd_pos");
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}


/**
  @brief
  ::info() is used to return information to the optimizer. See my_base.h for
  the complete description.

    @details
  Currently this table handler doesn't implement most of the fields really needed.
  SHOW also makes use of this data.

  You will probably want to have the following in your code:
    @code
  if (records < 2)
    records = 2;
    @endcode
  The reason is that the server will optimize for cases of only a single
  record. If, in a table scan, you don't know the number of records, it
  will probably be better to set records to two so you can return as many
  records as you need. Along with records, a few more variables you may wish
  to set are:
    records
    deleted
    data_file_length
    index_file_length
    delete_length
    check_time
  Take a look at the public variables in handler.h for more information.

  Called in filesort.cc, ha_heap.cc, item_sum.cc, opt_sum.cc, sql_delete.cc,
  sql_delete.cc, sql_derived.cc, sql_select.cc, sql_select.cc, sql_select.cc,
  sql_select.cc, sql_select.cc, sql_show.cc, sql_show.cc, sql_show.cc, sql_show.cc,
  sql_table.cc, sql_union.cc, and sql_update.cc.

    @see
  filesort.cc, ha_heap.cc, item_sum.cc, opt_sum.cc, sql_delete.cc, sql_delete.cc,
  sql_derived.cc, sql_select.cc, sql_select.cc, sql_select.cc, sql_select.cc,
  sql_select.cc, sql_show.cc, sql_show.cc, sql_show.cc, sql_show.cc, sql_table.cc,
  sql_union.cc and sql_update.cc
*/
int ha_vogal::info(uint flag)
{
  DBUG_ENTER("ha_vogal::info");
  DBUG_RETURN(0);
}


/**
  @brief
  This create a lock on the table. If you are implementing a storage engine
  that can handle transacations look at ha_berkely.cc to see how you will
  want to go about doing this. Otherwise you should consider calling flock()
  here. Hint: Read the section "locking functions for mysql" in lock.cc to understand
  this.

    @details
  Called from lock.cc by lock_external() and unlock_external(). Also called
  from sql_table.cc by copy_data_between_tables().

    @see
  lock.cc by lock_external() and unlock_external() in lock.cc;
  the section "locking functions for mysql" in lock.cc;
  copy_data_between_tables() in sql_table.cc.
*/
int ha_vogal::external_lock(THD *thd, int lock_type)
{
  DBUG_ENTER("ha_vogal::external_lock");
  DBUG_RETURN(0);
}


/**
  @brief
  The idea with handler::store_lock() is: The statement decides which locks
  should be needed for the table. For updates/deletes/inserts we get WRITE
  locks, for SELECT... we get read locks.

    @details
  Before adding the lock into the table lock handler (see thr_lock.c),
  mysqld calls store lock with the requested locks. Store lock can now
  modify a write lock to a read lock (or some other lock), ignore the
  lock (if we don't want to use MySQL table locks at all), or add locks
  for many tables (like we do when we are using a MERGE handler).

  Berkeley DB, for vogal, changes all WRITE locks to TL_WRITE_ALLOW_WRITE
  (which signals that we are doing WRITES, but are still allowing other
  readers and writers).

  When releasing locks, store_lock() is also called. In this case one
  usually doesn't have to do anything.

  In some exceptional cases MySQL may send a request for a TL_IGNORE;
  This means that we are requesting the same lock as last time and this
  should also be ignored. (This may happen when someone does a flush
  table when we have opened a part of the tables, in which case mysqld
  closes and reopens the tables and tries to get the same locks at last
  time). In the future we will probably try to remove this.

  Called from lock.cc by get_lock_data().

    @note
  In this method one should NEVER rely on table->in_use, it may, in fact,
  refer to a different thread! (this happens if get_lock_data() is called
  from mysql_lock_abort_for_thread() function)

    @see
  get_lock_data() in lock.cc
*/
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


/**
  @brief
  Used to delete a table. By the time delete_table() has been called all
  opened references to this table will have been closed (and your globally
  shared references released). The variable name will just be the name of
  the table. You will need to remove any files you have created at this point.

    @details
  If you do not implement this, the default delete_table() is called from
  handler.cc and it will delete all files with the file extensions returned
  by bas_ext().

  Called from handler.cc by delete_table and ha_create_table(). Only used
  during create if the table_flag HA_DROP_BEFORE_CREATE was specified for
  the storage engine.

    @see
  delete_table and ha_create_table() in handler.cc
*/
int ha_vogal::delete_table(const char *name)
{
	DBUG_ENTER("ha_vogal::delete_table");

	if (!vogal->getDefinition()->dropTable(const_cast<char*>(name))) {
		my_error(ER_CANT_REMOVE_ALL_FIELDS, MYF(0), "Erro ao remover a tabela");
		DBUG_RETURN(HA_ERR_GENERIC);
	}

	DBUG_RETURN(0);
}

int ha_vogal::create(const char *name, TABLE *table_arg,
                       HA_CREATE_INFO *create_info)
{
	DBUG_ENTER("ha_vogal::create");

	// Teste de DEBUG!!! Remover esta linha
	//vogal->ensureSanity();

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
