import scala.io.Source

object TestTables {
  val table1 : Table = new Table(
    List("col1", "col2"), List(
      List("a", "2"),
      List("b", "3"),
      List("c", "4"),
      List("d", "5")
    ))

  val table1String: String = {
    val src = Source.fromFile("tables/table1.csv")
    val str = src.mkString
    src.close()
    str.replace("\r", "")
  }

  val table2 : Table = {
    val src = Source.fromFile("tables/table2.csv")
    val str = src.mkString
    src.close()
    Table(str.replace("\r", ""))
  }

  val table3 : Table = {
    val src = Source.fromFile("tables/table3.csv")
    val str = src.mkString
    src.close()
    Table(str.replace("\r", ""))
  }

  val table4: Table = {
    val src = Source.fromFile("tables/table4.csv")
    val str = src.mkString
    src.close()
    Table(str.replace("\r", ""))
  }

  val table3_4_merged : Table = {
    val src = Source.fromFile("tables/table3_4_merged.csv")
    val str = src.mkString
    src.close()
    Table(str.replace("\r", ""))
  }

  val test3_newCol_Value : Table = {
    val src = Source.fromFile("tables/test_3_newCol_Value.csv")
    val str = src.mkString
    src.close()
    Table(str.replace("\r", ""))
  }

  val test3_Select_Value: Table = {
    val src = Source.fromFile("tables/test_3_Select_Value.csv")
    val str = src.mkString
    src.close()
    Table(str.replace("\r", ""))
  }

  val test3_Filter_Value: Table = {
    val src = Source.fromFile("tables/test_3_Filter_Value.csv")
    val str = src.mkString
    src.close()
    Table(str.replace("\r", ""))
  }

  val test3_Merge_Value: Table = {
    val src = Source.fromFile("tables/test_3_Merge_Value.csv")
    val str = src.mkString
    src.close()
    Table(str.replace("\r", ""))
  }

  val tableFunctional : Table = {
    val src = Source.fromFile("tables/Functional.csv")
    val str = src.mkString
    src.close()
    Table(str.replace("\r", ""))
  }

  val tableObjectOriented : Table = {
    val src = Source.fromFile("tables/Object-Oriented.csv")
    val str = src.mkString
    src.close()
    Table(str.replace("\r", ""))
  }

  val tableImperative : Table = {
    val src = Source.fromFile("tables/Imperative.csv")
    val str = src.mkString
    src.close()
    Table(str.replace("\r", ""))
  }

  val ref_programmingLanguages1: Table = {
    val src = Source.fromFile("tables/test_3_1.csv")
    val str = src.mkString
    src.close()
    Table(str.replace("\r", ""))
  }

  val ref_programmingLanguages2: Table = {
    val src = Source.fromFile("tables/test_3_2.csv")
    val str = src.mkString
    src.close()
    Table(str.replace("\r", ""))
  }

  val ref_programmingLanguages3: Table = {
    val src = Source.fromFile("tables/test_3_3.csv")
    val str = src.mkString
    src.close()
    Table(str.replace("\r", ""))
  }

  // 3.1
  def programmingLanguages1: Table = {
    val Func = tableFunctional.newCol("Functional", "Yes")
    val OO = tableObjectOriented.newCol("Object-Oriented", "Yes")
    val Imp = tableImperative.newCol("Imperative", "Yes")
    val mergedTables = Func.merge("Language", OO)
      .get.merge("Language", Imp)
    new Table(mergedTables.get.getColumnNames, mergedTables.get.getTabular)
  }


  // 3.2
  val programmingLanguages2: Table = {
    val table = programmingLanguages1.filter(And(
      Field("Original purpose", _.contains("Application")),
      Field("Other paradigms", _.contains("concurrent"))))

    new Table(table.get.getColumnNames, table.get.getTabular)
  }

  // 3.3
  val programmingLanguages3: Table = {
    val table = programmingLanguages2.select(List("Language", "Object-Oriented", "Functional"))
    new Table(table.get.getColumnNames, table.get.getTabular)
  }

}
