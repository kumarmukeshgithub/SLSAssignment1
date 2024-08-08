package com.klm.sls.processor.schema

case class CrewClass(
                      CabinClass: String,
                      XCM: Option[Long],
                      XFA: Option[Long]
                    )
