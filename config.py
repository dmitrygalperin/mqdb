class DbConfig(object):
    username     = 'dg94'
    password     = 'it490'
    host         = '192.168.56.101'
    db_type      = 'mysql'
    db_name      = 'testdb'

class MqConfig(object):
    username     = 'dg94'
    password     = 'it490'
    host         = '192.168.56.102'
    port         = 5672
    virtual_host = '/'
    queue        = 'db'
