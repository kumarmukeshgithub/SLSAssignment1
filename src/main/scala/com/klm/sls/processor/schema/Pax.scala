package com.klm.sls.processor.schema

case class Pax(
                Downgrades: Option[Long],
                ElectronicCoupons: Option[Long],
                NonRevenue: Option[Long],
                NonRevenueBabies: Option[Long],
                PaxClass: Seq[PaxClass],
                Upgrades: Option[Long]
              )
