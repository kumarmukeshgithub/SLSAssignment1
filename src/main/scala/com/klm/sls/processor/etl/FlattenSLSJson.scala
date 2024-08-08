package com.klm.sls.processor.etl

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object FlattenSLSJson {
  def SLSFlattenedJson(dataFrame: DataFrame): DataFrame = {

    val flattenedDF = dataFrame

      //Explode the Array Type
      .withColumn("Leg", explode(col("event.SlsValidatedNotificationEvent.Flight.Leg")))
      .withColumn("Segment", explode(col("event.SlsValidatedNotificationEvent.Flight.Segment")))
      .withColumn("CrewClass", explode(col("Segment.Crew.CrewClass")))
      .withColumn("PaxClass",explode(col("Segment.Pax.PaxClass")))
      .withColumn("LegCrewClass",explode(col("Leg.Crew.CrewClass")))
      .withColumn("LegPaxClass",explode(col("Leg.Pax.PaxClass")))
      .select(
        col("timestamp"),
        col("event.SlsValidatedNotificationEvent.Flight.Aircraft.AircraftOwner").alias("Flight_Aircraft_AircraftOwner"),
        col("event.SlsValidatedNotificationEvent.Flight.Aircraft.AircraftRegistration").alias("Flight_Aircraft_AircraftRegistration"),
        col("event.SlsValidatedNotificationEvent.Flight.Aircraft.AircraftSubType").alias("Flight_Aircraft_AircraftSubType"),
        col("event.SlsValidatedNotificationEvent.Flight.Aircraft.AircraftType").alias("Flight_Aircraft_AircraftType"),
        col("event.SlsValidatedNotificationEvent.Flight.Aircraft.DetailCode").alias("Flight_Aircraft_DetailCode"),
        col("event.SlsValidatedNotificationEvent.Flight.Aircraft.VersionCode").alias("Flight_Aircraft_VersionCode"),
        col("event.SlsValidatedNotificationEvent.Flight.AirlDsgCd").alias("Flight_AirlDsgCd"),
        col("event.SlsValidatedNotificationEvent.Flight.FltIdDt").alias("Flight_FltIdDt"),
        col("event.SlsValidatedNotificationEvent.Flight.FltNbr").alias("Flight_FltNbr"),
        col("event.SlsValidatedNotificationEvent.Flight.FlightInfo.ActOUTDt").alias("Flight_FlightInfo_ActOUTDt"),
        col("event.SlsValidatedNotificationEvent.Flight.FlightInfo.ActOUTDtLocal").alias("Flight_FlightInfo_ActOUTDtLocal"),
        col("event.SlsValidatedNotificationEvent.Flight.FlightInfo.Direction").alias("Flight_FlightInfo_Direction"),
        col("event.SlsValidatedNotificationEvent.Flight.FlightInfo.Route").alias("Flight_FlightInfo_Route"),
        col("event.SlsValidatedNotificationEvent.Flight.FlightInfo.SchOUTDt").alias("Flight_FlightInfo_SchOUTDt"),
        col("event.SlsValidatedNotificationEvent.Flight.FlightInfo.SchOUTDtLocal").alias("Flight_FlightInfo_SchOUTDtLocal"),

        col("Leg.Cargo.BlockedSpaceAgreement").alias("Leg_Cargo_BlockedSpaceAgreement"),
        col("Leg.Cargo.BlockedSpaceOccupied").alias("Leg_Cargo_BlockedSpaceOccupied"),
        col("Leg.Cargo.Cargo").alias("Leg_Cargo_Cargo"),
        col("Leg.Cargo.Mail").alias("Leg_Cargo_Mail"),
        col("Leg.Cargo.NonRevenueCargo").alias("Leg_Cargo_NonRevenueCargo"),
        col("Leg.Cargo.Pallets").alias("Leg_Cargo_Pallets"),
        col("Leg.Cargo.RealizedWeightCargo").alias("Leg_Cargo_RealizedWeightCargo"),
        col("Leg.SchArrStn").alias("Leg_SchArrStn"),
        col("Leg.SchDepStn").alias("Leg_SchDepStn"),

        col("LegCrewClass.CabinClass").alias("LegCrewClass_CabinClass"),
        col("LegCrewClass.XCM").alias("LegCrewClass_XCM"),
        col("LegCrewClass.XFA").alias("LegCrewClass_XFA"),

        col("LegPaxClass.Adults").alias("LegPaxClass_Adults"),
        col("LegPaxClass.BlockedSeatsAgreement").alias("LegPaxClass_BlockedSeatsAgreement"),
        col("LegPaxClass.CabinClass").alias("LegPaxClass_CabinClass"),
        col("LegPaxClass.Children").alias("LegPaxClass_Children"),
        col("LegPaxClass.NonRevenueAdults").alias("LegPaxClass_NonRevenueAdults"),
        col("LegPaxClass.NonRevenueChildren").alias("LegPaxClass_NonRevenueChildren"),
        col("LegPaxClass.Seats").alias("LegPaxClass_Seats"),


        col("Segment.SegmentDirection").alias("Segment_SegmentDirection"),
        col("Segment.SchArrStn").alias("Segment_SchArrStn"),
        col("Segment.SchDepStn").alias("Segment_SchDepStn"),

        col("Segment.Cargo.BlockedSpaceAgreement").alias("Segment_Cargo_BlockedSpaceAgreement"),
        col("Segment.Cargo.BlockedSpaceOccupied").alias("Segment_Cargo_BlockedSpaceOccupied"),
        col("Segment.Cargo.Cargo").alias("Segment_Cargo_Cargo"),
        col("Segment.Cargo.Mail").alias("Segment_Cargo_Mail"),
        col("Segment.Cargo.NonRevenueCargo").alias("Segment_Cargo_NonRevenueCargo"),
        col("Segment.Cargo.Pallets").alias("Segment_Cargo_Pallets"),
        col("Segment.Cargo.RealizedWeightCargo").alias("Segment_Cargo_RealizedWeightCargo"),

        col("Segment.Pax.Downgrades").alias("Segment_Pax_Downgrades"),
        col("Segment.Pax.ElectronicCoupons").alias("Segment_Pax_ElectronicCoupons"),
        col("Segment.Pax.NonRevenue").alias("Segment_Pax_NonRevenue"),
        col("Segment.Pax.NonRevenueBabies").alias("Segment_Pax_NonRevenueBabies"),
        col("Segment.Pax.Upgrades").alias("Segment_Pax_Upgrades"),

        col("CrewClass.CabinClass").alias("CrewClass_CabinClass"),
        col("CrewClass.XCM").alias("CrewClass_XCM"),
        col("CrewClass.XFA").alias("CrewClass_XFA"),

        col("PaxClass.Adults").alias("PaxClass_Adults"),
        col("PaxClass.BlockedSeatsAgreement").alias("PaxClass_BlockedSeatsAgreement"),
        col("PaxClass.CabinClass").alias("PaxClass_CabinClass"),
        col("PaxClass.Children").alias("PaxClass_Children"),
        col("PaxClass.NonRevenueAdults").alias("PaxClass_NonRevenueAdults"),
        col("PaxClass.NonRevenueChildren").alias("PaxClass_NonRevenueChildren"),
        col("PaxClass.Seats").alias("PaxClass_Seats")

      )
    flattenedDF
  }
}