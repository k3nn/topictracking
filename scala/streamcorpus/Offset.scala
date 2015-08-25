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
 * Offset specifies a range within a field of data in this ContentItem
 */
object Offset extends ThriftStructCodec3[Offset] {
  val Struct = new TStruct("Offset")
  val TypeField = new TField("type", TType.ENUM, 1)
  val FirstField = new TField("first", TType.I64, 2)
  val LengthField = new TField("length", TType.I32, 3)
  val XpathField = new TField("xpath", TType.STRING, 4)
  val ContentFormField = new TField("content_form", TType.STRING, 5)
  val ValueField = new TField("value", TType.STRING, 6)

  /**
   * Checks that all required fields are non-null.
   */
  def validate(_item: Offset) {
  }

  override def encode(_item: Offset, _oproto: TProtocol) { _item.write(_oproto) }
  override def decode(_iprot: TProtocol): Offset = Immutable.decode(_iprot)

  def apply(
    `type`: OffsetType,
    first: Long,
    length: Int,
    xpath: Option[String] = None,
    contentForm: String = "clean_visible",
    value: Option[ByteBuffer] = None
  ): Offset = new Immutable(
    `type`,
    first,
    length,
    xpath,
    contentForm,
    value
  )

  def unapply(_item: Offset): Option[Product6[OffsetType, Long, Int, Option[String], String, Option[ByteBuffer]]] = Some(_item)

  object Immutable extends ThriftStructCodec3[Offset] {
    override def encode(_item: Offset, _oproto: TProtocol) { _item.write(_oproto) }
    override def decode(_iprot: TProtocol): Offset = {
      var `type`: OffsetType = null
      var _got_type = false
      var first: Long = 0L
      var _got_first = false
      var length: Int = 0
      var _got_length = false
      var xpath: String = null
      var _got_xpath = false
      var contentForm: String = "clean_visible"
      var _got_contentForm = false
      var value: ByteBuffer = null
      var _got_value = false
      var _done = false
      _iprot.readStructBegin()
      while (!_done) {
        val _field = _iprot.readFieldBegin()
        if (_field.`type` == TType.STOP) {
          _done = true
        } else {
          _field.id match {
            case 1 => { /* `type` */
              _field.`type` match {
                case TType.I32 | TType.ENUM => {
                  `type` = {
                    streamcorpus.OffsetType(_iprot.readI32())
                  }
                  _got_type = true
                }
                case _ => TProtocolUtil.skip(_iprot, _field.`type`)
              }
            }
            case 2 => { /* first */
              _field.`type` match {
                case TType.I64 => {
                  first = {
                    _iprot.readI64()
                  }
                  _got_first = true
                }
                case _ => TProtocolUtil.skip(_iprot, _field.`type`)
              }
            }
            case 3 => { /* length */
              _field.`type` match {
                case TType.I32 => {
                  length = {
                    _iprot.readI32()
                  }
                  _got_length = true
                }
                case _ => TProtocolUtil.skip(_iprot, _field.`type`)
              }
            }
            case 4 => { /* xpath */
              _field.`type` match {
                case TType.STRING => {
                  xpath = {
                    _iprot.readString()
                  }
                  _got_xpath = true
                }
                case _ => TProtocolUtil.skip(_iprot, _field.`type`)
              }
            }
            case 5 => { /* contentForm */
              _field.`type` match {
                case TType.STRING => {
                  contentForm = {
                    _iprot.readString()
                  }
                  _got_contentForm = true
                }
                case _ => TProtocolUtil.skip(_iprot, _field.`type`)
              }
            }
            case 6 => { /* value */
              _field.`type` match {
                case TType.STRING => {
                  value = {
                    _iprot.readBinary()
                  }
                  _got_value = true
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
        `type`,
        first,
        length,
        if (_got_xpath) Some(xpath) else None,
        contentForm,
        if (_got_value) Some(value) else None
      )
    }
  }

  /**
   * The default read-only implementation of Offset.  You typically should not need to
   * directly reference this class; instead, use the Offset.apply method to construct
   * new instances.
   */
  class Immutable(
    val `type`: OffsetType,
    val first: Long,
    val length: Int,
    val xpath: Option[String] = None,
    val contentForm: String = "clean_visible",
    val value: Option[ByteBuffer] = None
  ) extends Offset

  /**
   * This Proxy trait allows you to extend the Offset trait with additional state or
   * behavior and implement the read-only methods from Offset using an underlying
   * instance.
   */
  trait Proxy extends Offset {
    protected def _underlying_Offset: Offset
    def `type`: OffsetType = _underlying_Offset.`type`
    def first: Long = _underlying_Offset.first
    def length: Int = _underlying_Offset.length
    def xpath: Option[String] = _underlying_Offset.xpath
    def contentForm: String = _underlying_Offset.contentForm
    def value: Option[ByteBuffer] = _underlying_Offset.value
  }
}

trait Offset extends ThriftStruct
  with Product6[OffsetType, Long, Int, Option[String], String, Option[ByteBuffer]]
  with java.io.Serializable
{
  import Offset._

  private[this] val TypeFieldI32 = new TField("type", TType.I32, 1)
  def withoutPassthroughs(f: TField => Boolean) = this
  def withPassthroughs(pts: TraversableOnce[(TField, TTransport)]) = this

  def `type`: OffsetType
  def first: Long
  def length: Int
  def xpath: Option[String]
  def contentForm: String
  def value: Option[ByteBuffer]

  def _1 = `type`
  def _2 = first
  def _3 = length
  def _4 = xpath
  def _5 = contentForm
  def _6 = value

  override def write(_oprot: TProtocol) {
    Offset.validate(this)
    _oprot.writeStructBegin(Struct)
    if (`type` ne null) {
      val type_item = `type`
      _oprot.writeFieldBegin(TypeFieldI32)
      _oprot.writeI32(type_item.value)
      _oprot.writeFieldEnd()
    }
    if (true) {
      val first_item = first
      _oprot.writeFieldBegin(FirstField)
      _oprot.writeI64(first_item)
      _oprot.writeFieldEnd()
    }
    if (true) {
      val length_item = length
      _oprot.writeFieldBegin(LengthField)
      _oprot.writeI32(length_item)
      _oprot.writeFieldEnd()
    }
    if (xpath.isDefined) {
      val xpath_item = xpath.get
      _oprot.writeFieldBegin(XpathField)
      _oprot.writeString(xpath_item)
      _oprot.writeFieldEnd()
    }
    if (contentForm ne null) {
      val contentForm_item = contentForm
      _oprot.writeFieldBegin(ContentFormField)
      _oprot.writeString(contentForm_item)
      _oprot.writeFieldEnd()
    }
    if (value.isDefined) {
      val value_item = value.get
      _oprot.writeFieldBegin(ValueField)
      _oprot.writeBinary(value_item)
      _oprot.writeFieldEnd()
    }
    _oprot.writeFieldStop()
    _oprot.writeStructEnd()
  }

  def copy(
    `type`: OffsetType = this.`type`, 
    first: Long = this.first, 
    length: Int = this.length, 
    xpath: Option[String] = this.xpath, 
    contentForm: String = this.contentForm, 
    value: Option[ByteBuffer] = this.value
  ): Offset =
    new Immutable(
      `type`, 
      first, 
      length, 
      xpath, 
      contentForm, 
      value
    )

  override def canEqual(other: Any): Boolean = other.isInstanceOf[Offset]

  override def equals(other: Any): Boolean = _root_.scala.runtime.ScalaRunTime._equals(this, other)

  override def hashCode: Int = _root_.scala.runtime.ScalaRunTime._hashCode(this)

  override def toString: String = _root_.scala.runtime.ScalaRunTime._toString(this)


  override def productArity: Int = 6

  override def productElement(n: Int): Any = n match {
    case 0 => `type`
    case 1 => first
    case 2 => length
    case 3 => xpath
    case 4 => contentForm
    case 5 => value
    case _ => throw new IndexOutOfBoundsException(n.toString)
  }

  override def productPrefix: String = "Offset"
}