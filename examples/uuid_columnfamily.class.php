<?php
/**
 * Example TimeUUIDType ColumnFamily vs default Cassandra storage-conf.xml
 *
 *  <ColumnFamily CompareWith="TimeUUIDType" Name="StandardByUUID1"/>
 *
 */
error_reporting(E_ALL);
ini_set('display_errors', true);
require_once('../config.php');
Pandra\Core::connect('default', 'localhost');

// ---- TIMEUUID ColumnFamily Example

$ks = 'Keyspace1';
$cfName = 'StandardByUUID1';
$keyID = 'PandraTestUUID1';

$cf = new Pandra\ColumnFamily($keyID,
                                $ks,
                                $cfName,
                                Pandra\ColumnFamily::TYPE_UUID);

// generate 5 timestamped columns
for ($i = 1; $i <= 5; $i++) {
    $cf->addColumn(Pandra\UUID::v1())->setValue($i);
}

echo 'Saving...<br>';
print_r($cf->toJSON());
$cf->save();

// get slice of the 5 most recent entries (count = 5, reversed = true)
echo '<br><br>Loading via CF container...<br>';
$cfNew = new Pandra\ColumnFamily($keyID,
                                    $ks,
                                    $cfName,
                                    Pandra\ColumnFamily::TYPE_UUID);
$cfNew->limit(5)->load();
echo '<br>Loaded...<br>';
print_r($cfNew->toJSON());

echo '<br><br>Loading Slice...<br>';
$result = Pandra\Core::getCFSlice($ks,
                                    $keyID,
                                    new cassandra_ColumnParent(array(
                                            'column_family' => $cfName
                                    )),
                                    new Pandra\SlicePredicate(
                                            Pandra\SlicePredicate::TYPE_RANGE,
                                            array('start' => '',
                                                    'finish' => '',
                                                    'count' => 5,
                                                    'reversed' => true))
                                    );

var_dump($result);

$cfNew = new Pandra\ColumnFamily($keyID,
                                    $ks,
                                    $cfName,
                                    Pandra\ColumnFamily::TYPE_UUID);
$cfNew->populate($result);

echo '<br>Imported...<br>';
print_r($cfNew->toJSON());



?>