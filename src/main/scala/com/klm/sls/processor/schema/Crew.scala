package com.klm.sls.processor.schema

case class Crew(
                 CrewClass: Seq[CrewClass],
                 DepartureSequenceNumber: Option[Long],
                 LegActOUTDt: Option[String],
                 LegDirection: Option[String],
                 LegSchOUTDt: Option[String]
               )
