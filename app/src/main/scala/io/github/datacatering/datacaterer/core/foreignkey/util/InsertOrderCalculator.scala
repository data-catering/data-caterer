package io.github.datacatering.datacaterer.core.foreignkey.util

import org.apache.log4j.Logger

import scala.collection.mutable

/**
 * Calculates the correct insertion order for foreign key relationships using topological sort.
 * Also detects circular dependencies and provides helpful error messages.
 */
object InsertOrderCalculator {

  private val LOGGER = Logger.getLogger(getClass.getName)

  /**
   * Calculate insertion order using topological sort.
   *
   * @param foreignKeys List of (parent, List[children]) relationships
   * @return Ordered list of data source names for proper FK insertion
   * @throws IllegalStateException if circular dependency detected
   */
  def getInsertOrder(foreignKeys: List[(String, List[String])]): List[String] = {
    // Step 1: Build graph (adjacency list) & track in-degrees
    val adjList = mutable.Map[String, List[String]]().withDefaultValue(List())
    val inDegree = mutable.Map[String, Int]().withDefaultValue(0)
    val allTables = mutable.Set[String]()

    foreignKeys.foreach { case (parent, children) =>
      allTables += parent
      children.foreach { child =>
        adjList.update(parent, adjList(parent) :+ child) // Preserve child order
        inDegree.update(child, inDegree(child) + 1)
        allTables += child
      }
    }

    // Step 2: Identify root nodes (in-degree == 0)
    val queue = mutable.Queue[String]()
    allTables.foreach { table =>
      if (inDegree(table) == 0) queue.enqueue(table)
    }

    // Step 3: Topological sort with child order preserved
    val result = mutable.ListBuffer[String]()
    while (queue.nonEmpty) {
      val table = queue.dequeue()
      result += table

      // Process children in defined order
      adjList(table).foreach { child =>
        inDegree.update(child, inDegree(child) - 1)
        if (inDegree(child) == 0) queue.enqueue(child)
      }
    }

    // Step 4: Check for circular dependencies
    if (result.size < allTables.size) {
      val cycleNodes = allTables.diff(result.toSet)
      val cycleInfo = detectCycle(adjList, cycleNodes.toList)

      LOGGER.error(s"Circular dependency detected in foreign key relationships!")
      LOGGER.error(s"Nodes involved in cycle: ${cycleNodes.mkString(", ")}")
      if (cycleInfo.nonEmpty) {
        LOGGER.error(s"Cycle path: ${cycleInfo.mkString(" -> ")}")
      }

      throw new IllegalStateException(
        s"Circular dependency detected in foreign key relationships. " +
        s"Nodes involved: ${cycleNodes.mkString(", ")}. " +
        s"Cycle path: ${cycleInfo.mkString(" -> ")}. " +
        s"Please review your foreign key definitions to break the cycle. " +
        s"Consider using nullable foreign keys or removing one of the relationships."
      )
    }

    result.toList
  }

  /**
   * Calculate deletion order (reverse of insertion order).
   *
   * @param foreignKeys List of (parent, List[children]) relationships
   * @return Ordered list of data source names for proper FK deletion (children first)
   */
  def getDeleteOrder(foreignKeys: List[(String, List[String])]): List[String] = {
    val fkMap = foreignKeys.toMap
    var visited = Set[String]()

    def getForeignKeyOrder(currKey: String): List[String] = {
      if (!visited.contains(currKey)) {
        visited = visited ++ Set(currKey)

        if (fkMap.contains(currKey)) {
          val children = foreignKeys.find(f => f._1 == currKey).map(_._2).getOrElse(List())
          val nested = children.flatMap(c => {
            if (!visited.contains(c)) {
              val nestedChildren = getForeignKeyOrder(c)
              visited = visited ++ Set(c)
              nestedChildren
            } else {
              List()
            }
          })
          nested ++ List(currKey)
        } else {
          List(currKey)
        }
      } else {
        List()
      }
    }

    foreignKeys.flatMap(x => getForeignKeyOrder(x._1))
  }

  /**
   * Detect and return a cycle in the foreign key dependency graph.
   * Uses DFS to find a back edge that indicates a cycle.
   *
   * @param adjList The adjacency list representing the FK dependency graph
   * @param startNodes Nodes suspected to be in a cycle
   * @return List of nodes forming a cycle, or empty if no cycle found
   */
  private def detectCycle(
    adjList: mutable.Map[String, List[String]],
    startNodes: List[String]
  ): List[String] = {
    val visited = mutable.Set[String]()
    val recStack = mutable.Set[String]()
    val path = mutable.ListBuffer[String]()

    def dfs(node: String): Option[List[String]] = {
      visited += node
      recStack += node
      path += node

      adjList.getOrElse(node, List()).foreach { neighbor =>
        if (!visited.contains(neighbor)) {
          dfs(neighbor) match {
            case Some(cycle) => return Some(cycle)
            case None =>
          }
        } else if (recStack.contains(neighbor)) {
          // Found a back edge - this is a cycle
          val cycleStart = path.indexOf(neighbor)
          return Some((path.slice(cycleStart, path.length) :+ neighbor).toList)
        }
      }

      recStack -= node
      path.remove(path.size - 1)
      None
    }

    // Try to find a cycle starting from each suspected node
    startNodes.foreach { node =>
      if (!visited.contains(node)) {
        dfs(node) match {
          case Some(cycle) => return cycle
          case None =>
        }
      }
    }

    List()
  }
}
