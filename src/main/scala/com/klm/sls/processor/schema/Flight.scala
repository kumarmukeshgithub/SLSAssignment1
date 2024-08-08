package com.klm.sls.processor.schema

case class Flight(
                   Aircraft: Aircraft,
                   AirlDsgCd: Option[String],
                   FlightInfo: FlightInfo,
                   FltIdDt: Option[String],
                   FltNbr: Option[String],
                   Leg: Seq[Leg],
                   Segment: Seq[Segment],
                   NotificationTimeStamp: Option[String],
                   Remark: Option[String],
                   SlsEventCode: Option[String]
                 )
