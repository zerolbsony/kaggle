kaggle中的所有影评数据量大，录入mysql中进行结构化存储。

一、数据库设计

1.电影详情表

```sql
CREATE TABLE netflix_movie (`
  `id int(10) unsigned NOT NULL COMMENT '电影id',`
  `release_year int(4) unsigned DEFAULT '0' COMMENT '电影发行年份',`
  `title varchar(255) DEFAULT '' COMMENT '电影标题',`
  `PRIMARY KEY (id)`
`) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='电影详情表';
```

2.影评分数表

```sql
CREATE TABLE netflix_rating (`
  `id int(10) NOT NULL AUTO_INCREMENT COMMENT '主键id',`
  `customer_id int(10) NOT NULL COMMENT '影评用户id',`
  `rating tinyint(1) DEFAULT NULL COMMENT '评分（1-5）',`
  `created date NOT NULL COMMENT '评价时间',`
  `movie_id int(10) NOT NULL COMMENT '电影id',`
  `PRIMARY KEY (id),`
  `KEY idx_movie_customer (movie_id,customer_id,created) USING BTREE`
`) ENGINE=InnoDB AUTO_INCREMENT=100481055 DEFAULT CHARSET=utf8mb4 COMMENT='影评分数表';
```

3.数据分析-电影特征表

```sql
CREATE TABLE statistic_movie_feature (`
  `id int(10) NOT NULL COMMENT '电影id',`
  `release_year int(4) unsigned DEFAULT '0' COMMENT '电影发行年份',`
  `title varchar(255) DEFAULT '' COMMENT '电影标题',`
  `rating_numbers int(10) unsigned DEFAULT '0' COMMENT '评分总次数',`
  `rating_avg_score tinyint(3) unsigned DEFAULT '0' COMMENT '评分平均分数',`
  `rating_scores int(10) unsigned DEFAULT '0' COMMENT '评分总分数',`
  `PRIMARY KEY (id)`
`) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='数据分析-电影特征表';
```

二、ETL

1.电影详情表和影评分数表

通过navicat数据导入工具完成，导入内容勾选忽略第一行即可。

2.数据分析-电影特征表

表字段目前只有简单的特征指标，通过spark sql算出。

在zeppelin新建notebook，输入以下：

```scala
%spark

import java.sql.DriverManager

val jdbcUrl = "jdbc:mysql://ip地址:3306/netflix_prize_data"
val mysqlUser = "root"
val mysqlPwd = "密码"

val jdbcDF = spark.read.format("jdbc").options(
    Map("url" -> jdbcUrl,
    "dbtable" -> "(select * from netflix_movie) t1",
    "driver" -> "com.mysql.jdbc.Driver",
    "user" -> mysqlUser,
    "password" -> mysqlPwd,
    "partitionColumn" -> "release_year",
    "lowerBound" -> "0",
    "upperBound" -> "1000",
    "numPartitions" -> "0",
    "fetchSize" -> "100"
    )).load()

// jdbcDF.show()
// jdbcDF.collect().take(20).foreach(println)

// jdbcDF.foreachPartition(
jdbcDF.collect().foreach(
    line => {
            println(line)
            println(line(0))
            //进行数据库链接
            val connection = DriverManager.getConnection(jdbcUrl,mysqlUser,mysqlPwd)
            try{
                //构建sql语句
                val sql =
                        "insert into statistic_movie_feature "+
                        "select t1.id id, t1.release_year release_year, t1.title title, "+
                        "count(distinct(t2.customer_id)) rating_numbers, "+
                        "avg(t2.rating) rating_avg_score, "+
                        "sum(t2.rating) rating_scores "+
                        "from netflix_movie t1 "+
                        "left join netflix_rating t2 "+
                        "on t1.id = t2.movie_id "+
                        "where t1.id = ? "+
                        "group by t2.movie_id,t1.id,t1.release_year, t1.title"
                //预备语句
                val ps = connection.prepareStatement(sql)
                //给每一个字段添加值
                // line.foreach(x=>{
                    ps.setInt(1,line(0).toString.toInt)
                //     //开始执行
                    ps.execute()
                // })
            }catch {
                case e:Exception => e.printStackTrace()
            }finally {
                if(connection != null){
                    connection.close()
                }
            }
    }
    )
```

三、生成推荐模型

1.通过python单机实现

依赖库：pandas, numpy, surprise, pymysql

大致流程如下：

 ![电影推荐流程](C:\Users\Administrator\Desktop\电影推荐流程.png)

透视表的结果示例，这里取样本时只选取了3个电影：

![image-20201112231859542](C:\Users\Administrator\AppData\Roaming\Typora\typora-user-images\image-20201112231859542.png)

流程中省略了训练过程，其实训练过程主要就是为了在训练样本中得到好的效果，得出调包时参数值填什么。

这里用RMSE和MAE来评估训练效果的偏差。

缺点：单机运行受限于空间和时间，需要通过执行多个任务来将所有样本集都参与计算，得出总的推荐结果并汇总后再取TopK。

2.通过spark实现

依赖库：spark的MLlib, jblas

大致流程如下：

![image-20201113003521824](C:\Users\Administrator\AppData\Roaming\Typora\typora-user-images\image-20201113003521824.png)

优点：可以借助spark的分布式计算得到全局结果，简化了代码的编写。