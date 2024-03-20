import util.Util.{Line, Row}

trait FilterCond {
  def &&(other: FilterCond): FilterCond = And(this, other)
  def ||(other: FilterCond): FilterCond = Or(this, other)
  // fails if the column name is not present in the row
  def eval(r: Row): Option[Boolean]
}
case class Field(colName: String, predicate: String => Boolean) extends FilterCond {
  // 2.2.
  override def eval(r: Row): Option[Boolean] = r.get(colName).map(predicate)
}

case class And(f1: FilterCond, f2: FilterCond) extends FilterCond {
  // 2.2.
  override def eval(r: Row): Option[Boolean] = {
    val r1 = f1.eval(r)
    val r2 = f2.eval(r)
    (r1, r2) match {
      case (Some(false), Some(false)) => Some(false)
      case (Some(false), Some(true)) => Some(false)
      case (Some(true), Some(false)) => Some(false)
      case (Some(true), Some(true)) => Some(true)
      case _ => None
    }
  }
}

case class Or(f1: FilterCond, f2: FilterCond) extends FilterCond {
  // 2.2.
  override def eval(r: Row): Option[Boolean] = {
    val r1 = f1.eval(r)
    val r2 = f2.eval(r)
    (r1, r2) match {
      case (Some(true), Some(true)) => Some(true)
      case (Some(true), Some(false)) => Some(true)
      case (Some(false), Some(true)) => Some(true)
      case (Some(false), Some(false)) => Some(false)
      case _ => None
    }
  }
}

trait Query {
  def eval: Option[Table]
}

/*
  Atom query which evaluates to the input table
  Always succeeds
 */
case class Value(t: Table) extends Query {
  override def eval: Option[Table] = Some(t)
}
/*
  Selects certain columns from the result of a target query
  Fails with None if some rows are not present in the resulting table
 */

case class Select(columns: Line, target: Query) extends Query {
  override def eval: Option[Table] =
    target.eval.get.select(columns)
}

/*
  Filters rows from the result of the target query
  Success depends only on the success of the target
 */
case class Filter(condition: FilterCond, target: Query) extends Query {
  override def eval: Option[Table] =
    target.eval.get.filter(condition)
}

/*
  Creates a new column with default values
  Success depends only on the success of the target
 */
case class NewCol(name: String, defaultVal: String, target: Query) extends Query {
  override def eval: Option[Table] =
    Some(target.eval.get.newCol(name, defaultVal))
}

/*
  Combines two tables based on a common key
  Success depends on whether the key exists in both tables or not AND on the success of the target
 */
case class Merge(key: String, t1: Query, t2: Query) extends Query {
  override def eval: Option[Table] = t1.eval.get.merge(key, t2.eval.get)
}


class Table (columnNames: Line, tabular: List[List[String]]) {
  def getColumnNames: Line = columnNames
  def getTabular: List[List[String]] = tabular

  // 1.1
  override def toString: String = {
    val columns = getColumnNames.mkString(",")
    val rows = getTabular.map(_.mkString(","))
    (columns :: rows).mkString("\n")
  }

  // 2.1
  def select(columns: Line): Option[Table] = {
    if (!columns.forall(getColumnNames.contains)) {
      None
    } else {
      val positions = columns.flatMap(s => getColumnNames.indexOf(s) match {
        case -1 => None
        case i => Some(i)
      })
      val selectedTabular = getTabular.map(row => positions.map(row))
      val selectedColumnNames = positions.map(getColumnNames)

      Some(new Table(selectedColumnNames, selectedTabular))
    }
  }

  // 2.2
  def filter(cond: FilterCond): Option[Table] = {
    def op(row: List[String], acc: List[List[String]]): List[List[String]] = {
      val map = getColumnNames.zip(row).toMap
      if (cond.eval(map).contains(true)) row :: acc else acc
    }
    val filteredRows = getTabular.foldRight(Nil: List[List[String]])(op)
    if (filteredRows.isEmpty) None else Some(new Table(getColumnNames, filteredRows))
  }

  // 2.3.
  def newCol(name: String, defaultVal: String): Table = {
    val col = List.fill(getTabular.size)(List(defaultVal))
    val resColNames = getColumnNames :+ name
    val resTabular = getTabular.zip(col).map {case (row1, row2) => row1 ++ row2}
    new Table(resColNames, resTabular)
  }

  def toListOfMaps: List[Map[String, String]] =
    getTabular.map(row => getColumnNames.zip(row).toMap)

  // 2.4.
  def merge(key: String, other: Table): Option[Table] = {
    val combinedColNames = (getColumnNames ++ other.getColumnNames).distinct
    val keyIdx = getColumnNames.indexOf(key)
    val otherKeyIdx = other.getColumnNames.indexOf(key)

    if (keyIdx == -1 || otherKeyIdx == -1) {
      None
    } else {
      val thisTableMaps = toListOfMaps
      val otherTableMaps = other.toListOfMaps

      val mergedTableMaps = thisTableMaps.flatMap { thisRow =>
        val keyVal = thisRow(key)
        val otherRows = otherTableMaps.filter(otherRow => otherRow(key) == keyVal)
        if (otherRows.isEmpty) {
          val mergedRow = combinedColNames.map(col => col -> thisRow.getOrElse(col, "")).toMap
          Some(mergedRow)
        } else {
          val mergedRow = otherRows.foldLeft(thisRow) { (merged, otherRow) =>
            combinedColNames.foldLeft(merged) { (row, col) =>
              val thisVal = row.getOrElse(col, "")
              val otherVal = otherRow.getOrElse(col, "")
              if (col != key) {
                val mergedVal = if (thisVal.isEmpty) otherVal else if (otherVal.isEmpty || thisVal == otherVal) thisVal else thisVal + ";" + otherVal
                row + (col -> mergedVal)
              } else {
                row
              }
            }
          }
          Some(mergedRow)
        }
      } ++ otherTableMaps.filterNot(otherRow => thisTableMaps.exists(thisRow => thisRow(key) == otherRow(key))).map { otherRow =>
        val mergedRow = combinedColNames.map(col => col -> otherRow.getOrElse(col, "")).toMap
        mergedRow
      }
      val mergedTableLists = mergedTableMaps.map(row => combinedColNames.map(col => row.getOrElse(col, "")))
      Some(new Table(combinedColNames, mergedTableLists))
    }
  }
}

object Table {
  // 1.2

  def apply(s: String): Table = {
    val lines = s.mkString.split('\n').toList
    val columnName: List[String] = lines.head.split(',').toList
    val tabular: List[List[String]] = lines.tail.map(row => row.split(",", -1).toList)

    new Table(columnName, tabular)
  }
}
