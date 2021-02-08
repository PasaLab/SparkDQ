package pasa.bigdata.dqlib.models.iforest

sealed abstract class IFNode extends Serializable {}

/***
  * IForest internal node with left and right child, splitting feature
  */
class IFInternalNode (
  val leftChild: IFNode,
  val rightChild: IFNode,
  val featureIndex: Int,
  val featureValue: Double) extends IFNode {}

/**
  * IForest leaf node with number of instances in it
  */
class IFLeafNode (
  val numInstance: Long) extends IFNode {}
