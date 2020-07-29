package org.neo4j.blob.impl

import java.io._
import java.net.URL
import org.apache.commons.io.IOUtils
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClientBuilder
import org.neo4j.blob.{Blob, BlobEntry, BlobId, BlobWithId, InputStreamSource, MimeType}
import org.neo4j.blob.util.StreamUtils._

object BlobIdFactory {
  val EMPTY = BlobId(-1L, -1L);

  def fromBytes(bytes: Array[Byte]): BlobId = {
    val is = new ByteArrayInputStream(bytes);
    BlobId(is.readLong(), is.readLong());
  }

  def readFromStream(is: InputStream): BlobId = {
    fromBytes(is.readBytes(16))
  }
}

object BlobFactory {
  private class BlobImpl(val streamSource: InputStreamSource, val length: Long, val mimeType: MimeType, val oid: Option[BlobId] = None)
    extends Blob with BlobWithId {
    def withId(nid: BlobId): BlobWithId = new BlobImpl(streamSource, length, mimeType, Some(nid));

    def id: BlobId = oid.get;

    def entry: BlobEntry = new BlobEntryImpl(id, length, mimeType);
  }

  private class BlobEntryImpl(val id: BlobId, val length: Long, val mimeType: MimeType)
    extends BlobEntry {
  }

  def makeStoredBlob(entry: BlobEntry, streamSource: InputStreamSource): BlobWithId =
    new BlobImpl(streamSource, entry.length, entry.mimeType, Some(entry.id))

  def withId(blob: Blob, id: BlobId): BlobWithId =
    new BlobImpl(blob.streamSource, blob.length, blob.mimeType, Some(id))

  def makeBlob(length: Long, mimeType: MimeType, streamSource: InputStreamSource): Blob =
    new BlobImpl(streamSource, length, mimeType);

  def makeEntry(id: BlobId, length: Long, mimeType: MimeType): BlobEntry =
    new BlobEntryImpl(id, length, mimeType);

  def makeEntry(id: BlobId, blob: Blob): BlobEntry =
    new BlobEntryImpl(id, blob.length, blob.mimeType);

  def fromBytes(bytes: Array[Byte]): Blob = {
    fromInputStreamSource(new InputStreamSource() {
      override def offerStream[T](consume: (InputStream) => T): T = {
        val fis = new ByteArrayInputStream(bytes);
        val t = consume(fis);
        fis.close();
        t;
      }
    }, bytes.length, Some(MimeTypeFactory.fromText("application/octet-stream")));
  }

  val EMPTY: Blob = fromBytes(Array[Byte]());

  def fromInputStreamSource(iss: InputStreamSource, length: Long, mimeType: Option[MimeType] = None): Blob = {
    new BlobImpl(iss,
      length,
      mimeType.getOrElse(MimeTypeFactory.guessMimeType(iss)));
  }

  def fromFile(file: File, mimeType: Option[MimeType] = None): Blob = {
    fromInputStreamSource(new InputStreamSource() {
      override def offerStream[T](consume: (InputStream) => T): T = {
        val fis = new FileInputStream(file);
        val t = consume(fis);
        fis.close();
        t;
      }
    },
      file.length(),
      mimeType);
  }

  def fromHttpURL(url: String): Blob = {
    val client = HttpClientBuilder.create().build();
    val get = new HttpGet(url);
    val resp = client.execute(get);
    val en = resp.getEntity;
    val blob = BlobFactory.fromInputStreamSource(new InputStreamSource() {
      override def offerStream[T](consume: (InputStream) => T): T = {
        val t = consume(en.getContent)
        client.close()
        t
      }
    }, en.getContentLength, Some(MimeTypeFactory.fromText(en.getContentType.getValue)));

    blob
  }

  def fromURL(url: String): Blob = {
    val p = "(?i)(http|https|file|ftp|ftps):\\/\\/[\\w\\-_]+(\\.[\\w\\-_]+)+([\\w\\-\\.,@?^=%&:/~\\+#]*[\\w\\-\\@?^=%&/~\\+#])?".r
    val uri = p.findFirstIn(url).getOrElse(url)

    val lower = uri.toLowerCase();
    if (lower.startsWith("http://") || lower.startsWith("https://")) {
      fromHttpURL(uri);
    }
    else if (lower.startsWith("file://")) {
      fromFile(new File(uri.substring(lower.indexOf("//") + 1)));
    }
    else {
      //ftp, ftps?
      fromBytes(IOUtils.toByteArray(new URL(uri)));
    }
  }
}

class InlineBlob(bytes: Array[Byte], val length: Long, val mimeType: MimeType)
  extends Blob {

  override val streamSource: InputStreamSource = new InputStreamSource() {
    override def offerStream[T](consume: (InputStream) => T): T = {
      val fis = new ByteArrayInputStream(bytes);
      val t = consume(fis);
      fis.close();
      t;
    }
  };
}