package com.klm.sls.processor.schema

case class Cargo(
                  BlockedSpaceAgreement: Option[Long],
                  BlockedSpaceOccupied: Option[Long],
                  Cargo: Option[Long],
                  Mail: Option[Long],
                  NonRevenueCargo: Option[Long],
                  Pallets: Option[Long],
                  RealizedWeightCargo: Option[Long]
                )


