package com.klm.sls.processor.schema

case class Leg(
                Cargo: Cargo,
                Crew: Crew,
                Pax: Pax,
                SchArrStn: Option[String],
                SchDepStn: Option[String]
              )
