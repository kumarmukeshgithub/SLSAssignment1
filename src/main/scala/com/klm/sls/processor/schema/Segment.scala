package com.klm.sls.processor.schema

case class Segment(
                    Cargo: Cargo,
                    Crew: Crew,
                    Pax: Pax,
                    SegmentDirection: Option[String],
                    SchArrStn: Option[String],
                    SchDepStn: Option[String]
                  )

