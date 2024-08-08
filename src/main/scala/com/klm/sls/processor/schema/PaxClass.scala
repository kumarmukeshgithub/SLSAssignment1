package com.klm.sls.processor.schema

case class PaxClass(
                     Adults: Option[Long],
                     BlockedSeatsAgreement: String,
                     CabinClass: String,
                     Children: Option[Long],
                     NonRevenueAdults: Option[Long],
                     NonRevenueChildren: Option[Long],
                     Seats: Option[Long]
                   )
