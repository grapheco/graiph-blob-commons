package org.neo4j.blob.impl

import java.io._
import java.net.URL
import org.apache.commons.io.IOUtils
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClientBuilder
import org.neo4j.blob.{Blob, BlobEntry, BlobId, InputStreamSource, ManagedBlob, MimeType}

object BlobFactory {
  val httpClient = HttpClientBuilder.create().build();

  private class BlobImpl(val streamSource: InputStreamSource, val length: Long, val mimeType: MimeType)
    extends Blob {
    def withId(id: BlobId): ManagedBlob = new ManagedBlobImpl(streamSource, length, mimeType, id);
  }

  private class BlobEntryImpl(val id: BlobId, val length: Long, val mimeType: MimeType)
    extends BlobEntry {
  }

  private class ManagedBlobImpl(val streamSource: InputStreamSource, override val length: Long, override val mimeType: MimeType, override val id: BlobId)
    extends BlobEntryImpl(id, length, mimeType) with ManagedBlob {
  }

  def makeStoredBlob(entry: BlobEntry, streamSource: InputStreamSource): ManagedBlob =
    new ManagedBlobImpl(streamSource, entry.length, entry.mimeType, entry.id)

  def withId(blob: Blob, id: BlobId): ManagedBlob =
    new ManagedBlobImpl(blob.streamSource, blob.length, blob.mimeType, id)

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
    val get = new HttpGet(url)
    var resp = httpClient.execute(get)
    val en = resp.getEntity

    BlobFactory.fromInputStreamSource(new InputStreamSource() {
      override def offerStream[T](consume: (InputStream) => T): T = {
        val t = if (resp != null) {
          consume(en.getContent)
        }
        else {
          resp = httpClient.execute(get)
          consume(resp.getEntity.getContent)
        }

        resp.close()
        resp = null
        t
      }
    }, en.getContentLength, Some(MimeTypeFactory.fromText(en.getContentType.getValue)))
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
      val fis = new ByteArrayInputStream(bytes)
      val t = consume(fis)
      fis.close()
      t
    }
  }
}