package org.neo4j.blob.util

import org.slf4j.LoggerFactory

trait Logging {
  val logger = LoggerFactory.getLogger(this.getClass);
}
