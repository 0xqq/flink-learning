package willem.weiyu.bigData.flink.table.source

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.sources.CsvTableSource

object ScalaCsvSource {

  def main(args: Array[String]): Unit = {
    // ***************
    // STREAMING QUERY
    // ***************
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // create a TableEnvironment for streaming queries
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    // ***********
    // BATCH QUERY
    // ***********
    //val bEnv = ExecutionEnvironment.getExecutionEnvironment
    // create a TableEnvironment for batch queries
    //val bTableEnv = TableEnvironment.getTableEnvironment(bEnv)

    val csvSource = CsvTableSource.builder()
      .path("E://db_info.csv")
      .field("id_", Types.INT)
      .field("db_ip", Types.STRING)
      .field("db_port", Types.STRING)
      .field("db_name", Types.STRING)
      .field("db_username", Types.STRING)
      .field("db_password", Types.STRING)
      .field("create_time", Types.STRING)
      .fieldDelimiter(" ")
      .lineDelimiter("\n")
      .ignoreParseErrors()
      .build()

    val table = tableEnv.registerTableSource("db_info",csvSource)

  }
}
