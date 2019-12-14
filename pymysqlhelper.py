"""MySQL Class
    author: https://github.com/shixue

    DB Instance：
        class DBTest(MySQLHelper):
            _dbconf = {
                "host": "localhost",
                "user": "root",
                "password": "root",
                "database": "test",
                "linkname": "con_local_test"
            }

        _db_test_read = DBTest()


        class DBTestSsh(MySQLHelper):
            _dbconf = {
                "host": "127.0.0.1",
                "user": "root",
                "password": "root",
                "database": "db_test",
                "linkname": "con_ssh_test"
            }
            _sshconf = {
                "host": "192.168.10.10",
                "user": "username",
                "password": "password",
                "mysqlhost": "mysqlhost",
                "keyname": "/Users/admin/.ssh/dev.key",  # Find in project if no path is specified
            }

        _db_test_write = DBTestSsh()


    Method：
        fetchone()
        fetchall()
        insert()
        execute()

    Example：
        ret = _db_test_read.fetchone({"table": "test_user", "where": "and id<100", "order": "id desc", "debug": 1})

"""
import pymysql
# use ssh, install sshtunnel
#from sshtunnel import SSHTunnelForwarder

class MySQLHelper(object):
    __instance = None
    __ssh_link = {}
    _sshconf = {}
    __dblink = {}
    __linkname = None
    __cursor = None

    def __init__(self, dbconf={}):
        """dbconfig dict"""
        self._dbconf = dbconf
        self._table = self._dbconf.get('table')
        self.__connection(self)

    @classmethod
    def __connection(cls, self):
        if not self._dbconf:
            self._dbconf = cls._dbconf
        kwargs = self._dbconf
        host = 'host' in kwargs and kwargs['host'] or 'localhost'
        port = 'port' in kwargs and kwargs['port'] or 3306
        user = 'user' in kwargs and kwargs['user'] or 'root'
        passwd = 'password' in kwargs and kwargs['password'] or ''
        db = 'database' in kwargs and kwargs['database'] or ''
        charset = 'charset' in kwargs and kwargs['charset'] or 'utf8'  # set db charset
        cls.__linkname = 'linkname' in kwargs and kwargs['linkname'] or 'linkname'
        try:
            if cls._sshconf:
                """SSH Connection"""
                _ssh = cls._sshconf
                cls.__ssh_link[_ssh['host']] = SSHTunnelForwarder(
                    ('host' in _ssh and _ssh['host'], 'port' in _ssh and _ssh['port'] or 22),
                    ssh_username=_ssh['user'], ssh_password=_ssh['password'],
                    remote_bind_address=(_ssh['mysqlhost'], 'mysqlport' in _ssh and _ssh['mysqlport'] or port),
                    ssh_pkey='keyname' in _ssh and _ssh['keyname'] or None
                )
                cls.__ssh_link[_ssh['host']].start()
                port = cls.__ssh_link[_ssh['host']].local_bind_port
            if not cls.__dblink.get(cls.__linkname):
                print(cls.__dblink.get(cls.__linkname))
                cls.__dblink[cls.__linkname] = pymysql.connect(
                    host=host,
                    port=port,
                    user=user,
                    passwd=passwd,
                    db=db,
                    charset=charset
                )
                cls.__cursor = cls.__dblink.get(cls.__linkname).cursor()
                print(cls.__dblink.get(cls.__linkname))
            if not cls.__cursor:
                cls.__cursor = cls.__dblink.get(cls.__linkname).cursor()

        except Exception as ex:
            cls.close()
            raise ex

    def fetchone(self, param):
        """fetchone
            param is dict
        """
        try:
            sqlStr = self.__parseparam(param)
            self.__cursor.execute(sqlStr)
            data = self.__cursor.fetchone()
            return data
        except Exception as ex:
            # log...
            raise ex

    def fetchall(self, param):
        try:
            sql = self.__parseparam(param)
            self.__cursor.execute(sql)
            data = self.__cursor.fetchall()
            return data
        except Exception as ex:
            # log...
            raise ex

    def insert(self, param):
        try:
            data = param.get('data')
            if not data or type(data) != dict:
                raise Exception('Paramter "data" not a dict', 0)
            if not param.get('table') and not self._table:
                raise Exception('Not set table name', 0)
            if param.get('table'):
                self._table = param.get('table')

            keys = []
            values = []
            for (k, v) in data.items():
                keys.append(k)
                v_type = type(v)
                if v_type == int or v.isnumeric():
                    values.append(v)
                elif v_type == bytes:
                    v = bytes.decode(v, encoding=param.get('encoding') or 'utf-8')  # trans db charset
                    values.append("'%s'" % v)
                else:
                    values.append("'%s'" % v)

            sql = "INSERT INTO %s(%s) VALUE(%s);"%(self._table, ','.join(keys), ','.join('%s' % s for s in values))
            if param.get('debug'):
                print("Debug: \n%s\n" % sql)
            return self.execute(sql)

        except Exception as ex:
            # log...
            raise ex

    def execute(self, sql, args=None):
        """sql  string"""
        try:
            ret = self.__cursor.execute(sql, args)
            if sql.lower().strip().find('insert') == 0:
                ret = self.__dblink[self.__linkname].insert_id()
            self.__dblink[self.__linkname].commit()
            return ret
        except Exception as ex:
            # log...
            raise ex

    def __parseparam(self, param):
        """Return sql string"""
        field = param.get('field')
        if not field:
            field = '*'
        if not param.get('table') and not self._table:
            raise Exception('Not set table name', 0)
        if param.get('table'):
            self._table = param.get('table')

        where = '1 '
        if param.get('where'):
            where += param.get('where')
        if param.get('group'):
            where += ' GROUP BY ' + param.get('group')
        if param.get('order'):
            where += ' ORDER BY ' + param.get('order')
        if param.get('startIndex'):
            where += ' LIMIT ' + param.get('startIndex') + ',' + param.get('limitNum')
        if not param.get('startIndex') and param.get('limitNum'):
            where += ' LIMIT ' + param.get('limitNum')

        sql = "SELECT %s FROM %s WHERE %s" % (field, self._table, where)
        if param.get('debug'):
            print("Debug: \n%s\n"% sql)
        return sql


    def getlink(self):
        return self.__dblink

    @classmethod
    def close(cls):
        if cls.__cursor:
            cls.__cursor.close()
            cls.__cursor = None
        if cls.__dblink.get(cls.__linkname):
            cls.__dblink[cls.__linkname].close()
            del cls.__dblink[cls.__linkname]
        if cls._sshconf and cls.__ssh_link.get(cls._sshconf.get('host')):
            cls.__ssh_link[cls._sshconf['host']].close()

    def __new__(cls, *args, **kwargs):
        if not cls.__instance:
            cls.__instance = object.__new__(cls)
        return cls.__instance

    def __del__(self):
        self.close()


if __name__ == '__main__':
    class DBTest(MySQLHelper):
        _dbconf = {
            "host": "localhost",
            "user": "root",
            "password": "root",
            "database": "test",
            "linkname": "con_local_test"
        }

    _db_test_read = DBTest()
    ret = _db_test_read.fetchone({"table": "test_user", "where": "and id<100", "order": "id desc", "debug": 1})
    print(ret)
    del(_db_test_read)
