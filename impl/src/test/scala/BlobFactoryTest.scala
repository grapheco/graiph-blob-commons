import java.io.{File, FileInputStream}
import java.net.URL

import org.apache.commons.io.IOUtils
import org.junit.{Assert, Test}
import org.neo4j.blob.Blob
import org.neo4j.blob.impl.BlobFactory

class BlobFactoryTest {
  @Test
  def testFileBlob(): Unit = {
    val file = new File("./testinput/bluejoe1.jpg")
    val blob = BlobFactory.fromFile(file)
    _testBlob(blob, file.length(), "image/jpeg", IOUtils.toByteArray(new FileInputStream(file)))
  }

  @Test
  def testHttpsBlob(): Unit = {
    val surl = "https://www.baidu.com/img/flexible/logo/pc/result.png"
    val blob = BlobFactory.fromHttpURL(surl)
    val url = new URL(surl)
    _testBlob(blob, 6617, "image/png", IOUtils.toByteArray(url))
  }

  private def _testBlob(blob: Blob, expectedLength: Long, expectedMimeType: String, content: Array[Byte]): Unit = {
    Assert.assertEquals(expectedLength, blob.length)
    Assert.assertEquals(expectedMimeType, blob.mimeType.text)

    blob.offerStream { is =>
      Assert.assertArrayEquals(content, IOUtils.toByteArray(is))
    }

    //test repeatable
    blob.offerStream { is =>
      Assert.assertArrayEquals(content, IOUtils.toByteArray(is))
    }
  }
}
