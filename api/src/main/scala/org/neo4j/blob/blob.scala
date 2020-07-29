package org.neo4j.blob

import java.io._
import org.apache.commons.codec.binary.Hex
import org.apache.commons.io.IOUtils
import org.neo4j.blob.util.StreamUtils
import org.neo4j.blob.util.StreamUtils._

trait InputStreamSource {
  /**
    * note close input stream after consuming
    */
  def offerStream[T](consume: (InputStream) => T): T;
}

trait BlobEntry {
  val id: BlobId;
  val length: Long;
  val mimeType: MimeType;
}

trait Blob extends Comparable[Blob] {
  val length: Long;
  val mimeType: MimeType;
  val streamSource: InputStreamSource;

  def offerStream[T](consume: (InputStream) => T): T = streamSource.offerStream(consume);

  def toBytes() = offerStream(IOUtils.toByteArray(_));

  def makeTempFile(): File = {
    offerStream((is) => {
      val f = File.createTempFile("blob-", ".bin");
      IOUtils.copy(is, new FileOutputStream(f));
      f;
    })
  }

  override def compareTo(o: Blob) = this.length.compareTo(o.length);

  override def toString = s"blob(length=${length},mime-type=${mimeType.text})";
}

//actually a 4-long values
case class BlobId(value1: Long, value2: Long) {
  val values = Array[Long](value1, value2);

  def asByteArray(): Array[Byte] = {
    StreamUtils.convertLongArray2ByteArray(values);
  }

  def asLiteralString(): String = {
    Hex.encodeHexString(asByteArray());
  }
}

trait BlobWithId extends Blob {
  def id: BlobId;

  def entry: BlobEntry;
}

object BlobId {
  val EMPTY = BlobId(-1L, -1L);

  def fromBytes(bytes: Array[Byte]): BlobId = {
    val is = new ByteArrayInputStream(bytes);
    BlobId(is.readLong(), is.readLong());
  }

  def readFromStream(is: InputStream): BlobId = {
    fromBytes(is.readBytes(16))
  }
}