<?php
/**
 * Created by PhpStorm.
 * User: 11005884
 * Date: 2019/11/11
 * Time: 15:56
 */
namespace jamesluo\thrifthbase;
$GLOBALS['THRIFT_ROOT'] = __DIR__;
require_once $GLOBALS['THRIFT_ROOT'] . '/Thrift/Transport/TTransport.php';
require_once $GLOBALS['THRIFT_ROOT'] . '/Thrift/Transport/TSocket.php';
require_once $GLOBALS['THRIFT_ROOT'] . '/Thrift/Protocol/TProtocol.php';
require_once $GLOBALS['THRIFT_ROOT'] . '/Thrift/Protocol/TBinaryProtocol.php';
require_once $GLOBALS['THRIFT_ROOT'] . '/Thrift/Protocol/TBinaryProtocolAccelerated.php';
require_once $GLOBALS['THRIFT_ROOT'] . '/Thrift/Transport/TBufferedTransport.php';
require_once $GLOBALS['THRIFT_ROOT'] . '/Thrift/Type/TMessageType.php';
require_once $GLOBALS['THRIFT_ROOT'] . '/Thrift/Factory/TStringFuncFactory.php';
require_once $GLOBALS['THRIFT_ROOT'] . '/Thrift/StringFunc/TStringFunc.php';
require_once $GLOBALS['THRIFT_ROOT'] . '/Thrift/StringFunc/Core.php';
require_once $GLOBALS['THRIFT_ROOT'] . '/Thrift/Type/TType.php';
require_once $GLOBALS['THRIFT_ROOT'] . '/Thrift/Exception/TException.php';
require_once $GLOBALS['THRIFT_ROOT'] . '/Thrift/Exception/TTransportException.php';
require_once $GLOBALS['THRIFT_ROOT'] . '/Thrift/Exception/TProtocolException.php';
require_once $GLOBALS['THRIFT_ROOT'] . '/Hbase/Types.php';
require_once $GLOBALS['THRIFT_ROOT'] . '/Hbase/Hbase.php';
use Thrift\Protocol\TBinaryProtocol;
use Thrift\Transport\TBufferedTransport;
use Thrift\Transport\TSocket;
use Hbase\HbaseClient;
use Hbase\ColumnDescriptor;
use Hbase\Mutation;
class Jhbase {
    protected  $host = '127.0.0.1';
    protected  $port = '9090';
    protected  $RecvTimeout = '20000';
    protected  $SendTimeout = '10000';
    protected  $client = null;
    public  function __construct($host='127.0.0.1',$port=9090,$RecvTimeout=10000,$SendTimeout=20000)
    {
            $this->host = $host;
            $this->port = $port;
            $this->RecvTimeout = $RecvTimeout;
            $this->SendTimeout = $SendTimeout;
    }
    protected  function  getClient(){
        $socket = new TSocket($this->host,$this->port);
        $socket->setSendTimeout($this->SendTimeout); // 发送超时，单位毫秒
        $socket->setRecvTimeout($this->RecvTimeout); // 接收超时，单位毫秒
        $transport = new TBufferedTransport($socket);
        $protocol = new TBinaryProtocol($transport);
        $client = new HbaseClient($protocol);
        $transport->open();
        return $client;
    }

    /**
     * list tables
     * @return mixed
     * @throws \Exception
     */
    public function listTable()
    {
        try {
            $tables = $this->getClient()->getTableNames();
            return $tables;
        } catch (\Exception $e) {
            throw  $e;
        }
    }

    /**
     * @param $tableName
     * @param $row
     * @param $value
     * @param $column
     * @param array $atrribute
     * @return mixed
     */
    public function put($tableName, $row, $value, $column, $atrribute=array())
    {
        try {
            $mutations = array(
                new Mutation(array(
                    'column' => $column,
                    'value' => $value
                )),
            );
            $data = $this->getClient()->mutateRow($tableName, $row, $mutations, $atrribute);
            return $data;
        } catch (Exception $e) {
            throw  $e;
        }
    }

    /**
     * @param $tablename
     * @param $row
     * @param array $atrribute
     * @return mixed
     * @throws \Exception
     */
    public function getRow($tablename, $row, $atrribute = array())
    {
        try {
            $data = $this->getClient()->getRow($tablename, $row, $atrribute);
            return $data;
        } catch (\Exception $e) {
            throw  $e;
        }
    }

    /**
     * 删除数据
     * @param $tablename
     * @param $row
     * @param array $atrribute
     * @return mixed
     * @throws \Exception
     */
    public function deleteAllRow($tablename, $row, $atrribute = array())
    {
        try {
            $data = $this->getClient()->deleteAllRow($tablename, $row, $atrribute);
            return $data;
        } catch (\Exception $e) {
            throw  $e;
        }
    }

    /**
     * @param $tablename
     * @param $startAndPrefix
     * @param null $columns
     * @param null $atrribute
     * @return \Hbase\scanner|int|mixed
     * @throws \Exception
     */
    public function scanneropenwithprefix($tablename, $startAndPrefix,$nbRows = 100,$columns=[],$atrribute=[])
    {
        try {
            $scan = $this->getClient()->scannerOpenWithPrefix($tablename, $startAndPrefix, $columns, $atrribute);
            $data = $this->getClient()->scannerGetList($scan, $nbRows);
            return $data;
        } catch (\Exception $e) {
            throw  $e;
        }
    }

    /**
     * @param $tablename
     * @param $startRow
     * @param $stopRow
     * @param int $nbRows
     * @param array $columns
     * @param array $attributes
     * @return \Hbase\a|\Hbase\TRowResult[]|mixed
     * @throws \Exception
     */
    public  function  scanneropenwithstop($tablename,$startRow,$stopRow,$nbRows=100,$columns=[],$attributes=[])
    {
        try {
            $scan = $this->getClient()->scannerOpenWithStop($tablename, $startRow,$stopRow,  $columns,  $attributes);
            $data = $this->getClient()->scannerGetList($scan, $nbRows);
            return $data;
        } catch (\Exception $e) {
            throw  $e;
        }
    }

    /**
     * @param $tablename
     * @param $row
     * @param $column
     * @param $value
     * @return int|mixed
     * @throws \Exception
     */
    public function atomicincrement($tablename, $row, $column, $value)
    {
        try {
            $data = $this->getClient()->atomicIncrement($tablename, $row, $column, $value);
            return $data;
        } catch (\Exception $e) {
            throw  $e;
        }
    }


}
