package com.klm.sls.processor.schema

case class Root(
                 event: Event,
                 timestamp: Option[String]
               )
