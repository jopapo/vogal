
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
  ulong index_flags(uint inx, uint part, bool all_parts) const;
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
  
  THR_LOCK_DATA **store_lock(THD *thd, THR_LOCK_DATA **to,
                             enum thr_lock_type lock_type);

};

#endif
