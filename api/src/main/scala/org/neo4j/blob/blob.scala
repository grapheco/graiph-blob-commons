package org.neo4j.blob

import java.io._
import org.apache.commons.codec.binary.Hex
import org.apache.commons.io.IOUtils
import org.neo4j.blob.util.StreamUtils

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

//actually a 4-long value
case class BlobId(value1: Long, value2: Long) {
  val values = Array[Long](value1, value2);

  def asByteArray(): Array[Byte] = {
    StreamUtils.convertLongArray2ByteArray(values);
  }

  def asLiteralString(): String = {
    Hex.encodeHexString(asByteArray());
  }
}

trait ManagedBlob extends Blob with BlobEntry {
}