from datetime import datetime
from typing import Tuple
from src.services.models.fire_event_datacube import *
from src.services.models.fire_event import *


@dataclass
class FireEventDataBundle:
    location: Location
    datetime_dim: DateTime
    incident: Incident
    detector: Detector
    suppression: Suppression
    fire_spread: FireSpread
    fire_origin: FireOrigin
    extinguishing_system: ExtinguishingSystem
    fact: FireEventFact


# Example function to transform a FireEvent instance into corresponding fact and dimension objects
def transform_fire_event(fire_event: FireEvent) -> FireEventDataBundle:
    location = Location(
        id=fire_event.ID + "_location",
        Address=fire_event.Address,
        City=fire_event.City,
        zipcode=fire_event.zipcode,
        neighborhood_district=fire_event.neighborhood_district,
        Supervisor_District=fire_event.Supervisor_District,
        point=fire_event.point,
    )

    datetime_dim = DateTime(
        id=fire_event.ID + "_datetime",
        Incident_Date=fire_event.Incident_Date,
        Alarm_DtTm=fire_event.Alarm_DtTm,
        Arrival_DtTm=fire_event.Arrival_DtTm,
        Close_DtTm=fire_event.Close_DtTm,
        data_as_of=fire_event.data_as_of,
        data_loaded_at=fire_event.data_loaded_at,
    )

    incident = Incident(
        Incident_Number=fire_event.Incident_Number,
        Exposure_Number=fire_event.Exposure_Number,
        Call_Number=fire_event.Call_Number,
        Battalion=fire_event.Battalion,
        Station_Area=fire_event.Station_Area,
        Box=fire_event.Box,
        First_Unit_On_Scene=fire_event.First_Unit_On_Scene,
        Primary_Situation=fire_event.Primary_Situation,
        Mutual_Aid=fire_event.Mutual_Aid,
    )

    detector = Detector(
        id=fire_event.ID + "_detector",
        Detectors_Present=fire_event.Detectors_Present,
        Detector_Type=fire_event.Detector_Type,
        Detector_Operation=fire_event.Detector_Operation,
        Detector_Effectiveness=fire_event.Detector_Effectiveness,
        Detector_Failure_Reason=fire_event.Detector_Failure_Reason,
    )

    suppression = Suppression(
        id=fire_event.ID + "_suppression",
        Suppression_Units=fire_event.Suppression_Units,
        Suppression_Personnel=fire_event.Suppression_Personnel,
        EMS_Units=fire_event.EMS_Units,
        EMS_Personnel=fire_event.EMS_Personnel,
        Other_Units=fire_event.Other_Units,
        Other_Personnel=fire_event.Other_Personnel,
    )

    fire_spread = FireSpread(
        id=fire_event.ID + "_fire_spread",
        Fire_Spread=fire_event.Fire_Spread,
        No_Flame_Spread=fire_event.No_Flame_Spread,
        Number_of_floors_with_minimum_damage=fire_event.Number_of_floors_with_minimum_damage,
        Number_of_floors_with_significant_damage=fire_event.Number_of_floors_with_significant_damage,
        Number_of_floors_with_heavy_damage=fire_event.Number_of_floors_with_heavy_damage,
        Number_of_floors_with_extreme_damage=fire_event.Number_of_floors_with_extreme_damage,
    )

    fire_origin = FireOrigin(
        id=fire_event.ID + "_fire_origin",
        Area_of_Fire_Origin=fire_event.Area_of_Fire_Origin,
        Ignition_Cause=fire_event.Ignition_Cause,
        Ignition_Factor_Primary=fire_event.Ignition_Factor_Primary,
        Ignition_Factor_Secondary=fire_event.Ignition_Factor_Secondary,
        Heat_Source=fire_event.Heat_Source,
        Item_First_Ignited=fire_event.Item_First_Ignited,
        Human_Factors_Associated_with_Ignition=fire_event.Human_Factors_Associated_with_Ignition,
        Structure_Type=fire_event.Structure_Type,
        Structure_Status=fire_event.Structure_Status,
        Floor_of_Fire_Origin=fire_event.Floor_of_Fire_Origin,
    )

    extinguishing_system = ExtinguishingSystem(
        id=fire_event.ID + "_extinguishing_system",
        Automatic_Extinguishing_System_Present=fire_event.Automatic_Extinguishing_System_Present,
        Automatic_Extinguishing_System_Type=fire_event.Automatic_Extinguishing_System_Type,
        Automatic_Extinguishing_System_Perfomance=fire_event.Automatic_Extinguishing_System_Perfomance,
        Automatic_Extinguishing_System_Failure_Reason=fire_event.Automatic_Extinguishing_System_Failure_Reason,
        Number_of_Sprinkler_Heads_Operating=fire_event.Number_of_Sprinkler_Heads_Operating,
    )

    fact = FireEventFact(
        id=fire_event.ID,
        location_id=location.id,
        datetime_id=datetime_dim.id,
        incident_id=incident.Incident_Number,
        detector_id=detector.id,
        suppression_id=suppression.id,
        fire_spread_id=fire_spread.id,
        fire_origin_id=fire_origin.id,
        extinguishing_system_id=extinguishing_system.id,
        Fire_Fatalities=fire_event.Fire_Fatalities,
        Fire_Injuries=fire_event.Fire_Injuries,
        Civilian_Fatalities=fire_event.Civilian_Fatalities,
        Civilian_Injuries=fire_event.Civilian_Injuries,
        Estimated_Property_Loss=fire_event.Estimated_Property_Loss,
        Estimated_Contents_Loss=fire_event.Estimated_Contents_Loss,
        Number_of_Alarms=fire_event.Number_of_Alarms,
        Action_Taken_Primary=fire_event.Action_Taken_Primary,
        Action_Taken_Secondary=fire_event.Action_Taken_Secondary,
        Action_Taken_Other=fire_event.Action_Taken_Other,
        Detector_Alerted_Occupants=fire_event.Detector_Alerted_Occupants,
    )

    return FireEventDataBundle(
        location=location,
        datetime_dim=datetime_dim,
        incident=incident,
        detector=detector,
        suppression=suppression,
        fire_spread=fire_spread,
        fire_origin=fire_origin,
        extinguishing_system=extinguishing_system,
        fact=fact,
    )
