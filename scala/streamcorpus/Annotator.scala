/**
 * generated by Scrooge ${project.version}
 */
package streamcorpus

import com.twitter.scrooge.{
  ThriftException, ThriftStruct, ThriftStructCodec3}
import org.apache.thrift.protocol._
import org.apache.thrift.transport.{TMemoryBuffer, TTransport}
import java.nio.ByteBuffer
import scala.collection.mutable
import scala.collection.{Map, Set}

/**
 * An Annotator object describes a human (or possibly a set of humans)
 * who generated the data stored in a Label or Rating object.
 */
object Annotator extends ThriftStructCodec3[Annotator] {
  val Struct = new TStruct("Annotator")
  val AnnotatorIdField = new TField("annotator_id", TType.STRING, 1)
  val AnnotationTimeField = new TField("annotation_time", TType.STRUCT, 2)

  /**
   * Checks that all required fields are non-null.
   */
  def validate(_item: Annotator) {
  }

  override def encode(_item: Annotator, _oproto: TProtocol) { _item.write(_oproto) }
  override def decode(_iprot: TProtocol): Annotator = Immutable.decode(_iprot)

  def apply(
    annotatorId: String,
    annotationTime: Option[StreamTime] = None
  ): Annotator = new Immutable(
    annotatorId,
    annotationTime
  )

  def unapply(_item: Annotator): Option[Product2[String, Option[StreamTime]]] = Some(_item)

  object Immutable extends ThriftStructCodec3[Annotator] {
    override def encode(_item: Annotator, _oproto: TProtocol) { _item.write(_oproto) }
    override def decode(_iprot: TProtocol): Annotator = {
      var annotatorId: String = null
      var _got_annotatorId = false
      var annotationTime: StreamTime = null
      var _got_annotationTime = false
      var _done = false
      _iprot.readStructBegin()
      while (!_done) {
        val _field = _iprot.readFieldBegin()
        if (_field.`type` == TType.STOP) {
          _done = true
        } else {
          _field.id match {
            case 1 => { /* annotatorId */
              _field.`type` match {
                case TType.STRING => {
                  annotatorId = {
                    _iprot.readString()
                  }
                  _got_annotatorId = true
                }
                case _ => TProtocolUtil.skip(_iprot, _field.`type`)
              }
            }
            case 2 => { /* annotationTime */
              _field.`type` match {
                case TType.STRUCT => {
                  annotationTime = {
                    streamcorpus.StreamTime.decode(_iprot)
                  }
                  _got_annotationTime = true
                }
                case _ => TProtocolUtil.skip(_iprot, _field.`type`)
              }
            }
            case _ =>
              TProtocolUtil.skip(_iprot, _field.`type`)
          }
          _iprot.readFieldEnd()
        }
      }
      _iprot.readStructEnd()
      new Immutable(
        annotatorId,
        if (_got_annotationTime) Some(annotationTime) else None
      )
    }
  }

  /**
   * The default read-only implementation of Annotator.  You typically should not need to
   * directly reference this class; instead, use the Annotator.apply method to construct
   * new instances.
   */
  class Immutable(
    val annotatorId: String,
    val annotationTime: Option[StreamTime] = None
  ) extends Annotator

  /**
   * This Proxy trait allows you to extend the Annotator trait with additional state or
   * behavior and implement the read-only methods from Annotator using an underlying
   * instance.
   */
  trait Proxy extends Annotator {
    protected def _underlying_Annotator: Annotator
    def annotatorId: String = _underlying_Annotator.annotatorId
    def annotationTime: Option[StreamTime] = _underlying_Annotator.annotationTime
  }
}

trait Annotator extends ThriftStruct
  with Product2[String, Option[StreamTime]]
  with java.io.Serializable
{
  import Annotator._

  def withoutPassthroughs(f: TField => Boolean) = this
  def withPassthroughs(pts: TraversableOnce[(TField, TTransport)]) = this

  def annotatorId: String
  def annotationTime: Option[StreamTime]

  def _1 = annotatorId
  def _2 = annotationTime

  override def write(_oprot: TProtocol) {
    Annotator.validate(this)
    _oprot.writeStructBegin(Struct)
    if (annotatorId ne null) {
      val annotatorId_item = annotatorId
      _oprot.writeFieldBegin(AnnotatorIdField)
      _oprot.writeString(annotatorId_item)
      _oprot.writeFieldEnd()
    }
    if (annotationTime.isDefined) {
      val annotationTime_item = annotationTime.get
      _oprot.writeFieldBegin(AnnotationTimeField)
      annotationTime_item.write(_oprot)
      _oprot.writeFieldEnd()
    }
    _oprot.writeFieldStop()
    _oprot.writeStructEnd()
  }

  def copy(
    annotatorId: String = this.annotatorId, 
    annotationTime: Option[StreamTime] = this.annotationTime
  ): Annotator =
    new Immutable(
      annotatorId, 
      annotationTime
    )

  override def canEqual(other: Any): Boolean = other.isInstanceOf[Annotator]

  override def equals(other: Any): Boolean = _root_.scala.runtime.ScalaRunTime._equals(this, other)

  override def hashCode: Int = _root_.scala.runtime.ScalaRunTime._hashCode(this)

  override def toString: String = _root_.scala.runtime.ScalaRunTime._toString(this)


  override def productArity: Int = 2

  override def productElement(n: Int): Any = n match {
    case 0 => annotatorId
    case 1 => annotationTime
    case _ => throw new IndexOutOfBoundsException(n.toString)
  }

  override def productPrefix: String = "Annotator"
}