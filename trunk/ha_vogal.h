
#ifndef HA_VOGAL
#define HA_VOGAL

#ifdef USE_PRAGMA_INTERFACE
#pragma interface			/* gcc class implementation */
#endif

#include "vogal_handler.h"
#include <thr_lock.h>

// Classe gerenciadora dos módulos
static vogal_handler * vogal = NULL;

typedef struct {
  char *table_name;
  uint table_name_length, use_count;
  pthread_mutex_t mutex;
  THR_LOCK lock;
  CursorType *cursor;
  FilterCursorType *filter;
} VOGAL_SHARE;

class ha_vogal: public handler
{
  THR_LOCK_DATA lock;
  VOGAL_SHARE *share;
  
public:
  ha_vogal(handlerton *hton, TABLE_SHARE *table_arg);
  ~ha_vogal();

  const char *table_type() const;
  const char **bas_ext() const;
  ulonglong table_flags() const;
  uint max_supported_record_length() const;
  virtual double scan_time();

  int open(const char *name, int mode, uint test_if_locked);
  int close(void);
  int write_row(uchar *buf);
  int update_row(const uchar *old_data, uchar *new_data);
  int delete_row(const uchar *buf);

  int rnd_init(bool scan);
  int rnd_end();
  int rnd_next(uchar *buf);
  int rnd_pos(uchar *buf, uchar *pos);
  void position(const uchar *record);
  int info(uint);
  int external_lock(THD *thd, int lock_type);
  int delete_table(const char *from);
  int create(const char *name, TABLE *form,
             HA_CREATE_INFO *create_info);

  // Obs.: Implementação fundamental para utilização de índices
  ulong index_flags(uint inx, uint part, bool all_parts) const;
  /*const char *index_type(uint inx);
  uint max_supported_keys() const;
  uint max_supported_key_parts() const;
  uint max_supported_key_length() const;
  virtual double read_time(ha_rows rows);
  int index_read_map(uchar *buf, const uchar *key, key_part_map keypart_map, enum ha_rkey_function find_flag);
  int index_next(uchar *buf);
  int index_prev(uchar *buf);
  int index_first(uchar *buf);
  int index_last(uchar *buf);*/
  
  THR_LOCK_DATA **store_lock(THD *thd, THR_LOCK_DATA **to,
                             enum thr_lock_type lock_type);

  // Auxiliares
  int vogal2mysql(CursorType * cursor, RidCursorType * rid);

private:
	BigNumber m_UpdatedRid; // Auxiliar para criar registro com mesmo RID
  
};

#endif
