package relation
package compiler

import scala.collection.mutable

import ch.epfl.data.sc.pardis
import pardis.optimization.RecursiveRuleBasedTransformer
import pardis.quasi.TypeParameters._
import pardis.types._
import PardisTypeImplicits._
import pardis.ir._

import relation.deep.RelationDSLOpsPackaged
import relation.shallow._

/** ***************************************
  *         DAT Database Systems
  *              Assignment 2
  *        Cyriaque Brousse 227209
  * ***************************************/
class ColumnStoreLowering(override val IR: RelationDSLOpsPackaged, override val schemaAnalysis: SchemaAnalysis) extends RelationLowering(IR, schemaAnalysis) {
  import IR.Predef._

  type Value = String
  type LoweredRelation = Rep[Array[Array[Value]]]

  def relationScan(scanner: Rep[RelationScanner], schema: Schema, size: Rep[Int], resultSchema: Schema): LoweredRelation = {
    dsl"""
      val numCols = ${schema.size}
      val arr = new Array[Array[Value]](numCols)

      for (i <- 0 until numCols) {
        arr(i) = new Array[Value]($size)
      }

      var i = 0
      while ($scanner.hasNext) {
        var j = 0
        while(j < numCols) {
          arr(j)(i) = $scanner.next_string()
          j += 1
        }

        i += 1
      }

      arr
    """
  }
  
  def relationProject(relation: Rep[Relation], schema: Schema, resultSchema: Schema): LoweredRelation = {
    val arr = getRelationLowered(relation)
    val size = dsl"$arr(0).length"
    val oldSchema = getRelationSchema(relation)
    val matchCols = schema.columns.map(c => oldSchema indexOf c).toArray

    // array initialization
    val proj = dsl"""
      val newNumCols = ${schema.size}
      new Array[Array[Value]](newNumCols)
    """

    // deep copy for matching columns
    var i = 0
    for (c <- matchCols) {
      dsl"""
        $proj($i) = new Array[Value]($size)
        for (j <- 0 until $size) {
          $proj($i)(j) = $arr($c)(j)
        }
      """
      i += 1
    }

    proj
  }
  
  def relationSelect(relation: Rep[Relation], field: String, value: Rep[String], resultSchema: Schema): LoweredRelation = {
    val arr = getRelationLowered(relation)
    val size = dsl"$arr(0).length"
    val schema = getRelationSchema(relation)
    val numCols = schema.size
    val filtCol = schema indexOf field

    // array initialization
    val sel = dsl"new Array[Array[Value]]($numCols)"

    // deep copy and filtering
    dsl"""
      var newSize = 0
      for (j <- 0 until $size) {
        val e = $arr($filtCol)(j)
        if (e == $value) newSize += 1
      }

      for (c <- 0 until $numCols) {
        $sel(c) = new Array[Value](newSize)

        var r = 0
        for (j <- 0 until $size) {
          val e = $arr($filtCol)(j)
          if (e == $value) {
            $sel(c)(r) = $arr(c)(j)
            r += 1
          }
        }
      }
    """

    sel
  }
  
  def relationJoin(leftRelation: Rep[Relation], rightRelation: Rep[Relation], leftKey: String, rightKey: String, resultSchema: Schema): LoweredRelation = {
    val arrLhs     = getRelationLowered(leftRelation)
    val arrRhs     = getRelationLowered(rightRelation)
    val colsLhs    = getRelationSchema(leftRelation).columns
    val colsRhs    = getRelationSchema(rightRelation).columns filter (_ != rightKey)
    val numColsLhs = colsLhs.size
    val numColsRhs = colsRhs.size
    val sizeLhs    = dsl"$arrLhs(0).length"
    val sizeRhs    = dsl"$arrRhs(0).length"
    val numCols    = colsLhs.size + colsRhs.size
    val lhsFiltCol = getRelationSchema(leftRelation) indexOf leftKey
    val rhsFiltCol = getRelationSchema(rightRelation) indexOf rightKey

    // array initialization
    val join = dsl"new Array[Array[Value]]($numCols)"
    dsl"""
      var newSize = 0
      for (x <- 0 until $sizeLhs) {
        val leftFiltValue = $arrLhs($lhsFiltCol)(x)
        for (y <- 0 until $sizeRhs) {
          if (leftFiltValue == $arrRhs($rhsFiltCol)(y)) newSize += 1
        }
      }

      for (c <- 0 until $numCols) $join(c) = new Array[Value](newSize)
    """

    // joining
    dsl"""
      var r = 0
      for (x <- 0 until $sizeLhs) {
        val leftFiltValue = $arrLhs($lhsFiltCol)(x)
        for (y <- 0 until $sizeRhs) {
          if (leftFiltValue == $arrRhs($rhsFiltCol)(y)) {

            // copy from left array
            for (c <- 0 until $numColsLhs) {
              $join(c)(r) = $arrLhs(c)(x)
            }

            // copy from right array
            // take caution when encountering join column
            var offset = 0
            for (c <- $numColsLhs until $numCols) {
              val _c = c - $numColsLhs
              if (_c == $rhsFiltCol || offset != 0) {
                offset = 1
              }
              $join(c)(r) = $arrRhs(_c + offset)(y)
            }

            r += 1
          }
        }
      }
    """

    join
  }
  
  def relationPrint(relation: Rep[Relation]): Unit = {
    val arr = getRelationLowered(relation)
    val schema = getRelationSchema(relation)

    val stringForIndex = (i: Rep[Int]) => {
      dsl"""
        val numCols = ${schema.size}
        var res = ""
        for (c <- 0 until numCols) {
          res += $arr(c)($i)
          if (c < numCols - 1) res += "|"
        }
        res
      """
    }

    dsl" for (i <- 0 until $arr(0).length) println($stringForIndex(i))"
  }
  
}
