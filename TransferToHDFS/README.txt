It is an alternate utility to FTP/SFTP where you can send files directly from a non hadoop system with out having hadoop clients directly to HDFS , protocal userd is webHDFS.
It takes simple argument like username, password, file location local , file location in hdfs etc.
HDFS does  not generally have an authentication but this utility provides a way to save username / password in database and authenticate user against ity before taking the hadoop action.
The Authenticator jsp also send back the active namenode name for an HA system by connecting to the zookeepr and reading the znode for HA.
Once the namenode infor is ontained , rest is all handled by curl.
For static namenode just hardcode into the jsp. You have to search and replace the strings for namenodes and configs. Yes i did not write a very configurable script cause it was somepoint earleir when i did this but just thought of putting it here in case it is usefull to some one.
you would have to build the mysql table to keep user name and password, for me i kep a table like below:-

mysql> show create table user_details;
+--------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| Table        | Create Table                                                                                                                                                                                                                                                   |
+--------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| user_details | CREATE TABLE `user_details` (
  `user_name` varchar(20) DEFAULT NULL,
  `pass` varchar(20) DEFAULT NULL,
  `status` varchar(10) DEFAULT NULL,
  `cluster_name` varchar(20) DEFAULT NULL,
  KEY `idx_user` (`user_name`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1 |
+--------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
1 row in set (0.01 sec)

You would need to host the authenticator jsp on a tomcat webser or anything that supports jsp. The zookeeper clients also needs to be present on the machine where the jsp would run.Check out the jsp to configure the details