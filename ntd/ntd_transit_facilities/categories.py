from enum import Enum

class CATEGORIES(Enum):
    SURFACE_PARKING = 0
    PARKING_STRUCTURE = 1
    TRANSIT_STATION = 2
    ADMINISTRATIVE = 3
    MAINTENANCE = 4
    FUELING = 5
    OTHER_ADMIN_MAINTENANCE = 6
    OTHER_PASSENGER_PARKING = 7
    OTHER_UNCATEGORIZED = 8
    
COLORS_BY_CATEGORY = {
    CATEGORIES.SURFACE_PARKING: "#ababd9",
    CATEGORIES.PARKING_STRUCTURE: "#685bc7",
    CATEGORIES.TRANSIT_STATION: "#67d2df",
    CATEGORIES.ADMINISTRATIVE: "#ffb81c",
    CATEGORIES.MAINTENANCE: "#008675",
    CATEGORIES.FUELING: "#85ce8a",
    CATEGORIES.OTHER_ADMIN_MAINTENANCE: "#7d848c",
    CATEGORIES.OTHER_PASSENGER_PARKING: "#4b4f54",
    CATEGORIES.OTHER_UNCATEGORIZED: "#162933",
}
STRING_VALUE_BY_CATEGORY = {
    CATEGORIES.SURFACE_PARKING: "Surface Parking",
    CATEGORIES.PARKING_STRUCTURE: "Parking Structure",
    CATEGORIES.TRANSIT_STATION: "Transit Station",
    CATEGORIES.ADMINISTRATIVE: "Administrative",
    CATEGORIES.MAINTENANCE: "Maintenance",
    CATEGORIES.FUELING: "Fueling",
    CATEGORIES.OTHER_ADMIN_MAINTENANCE: "Other, Admin / Maintenance",
    CATEGORIES.OTHER_PASSENGER_PARKING: "Other, Passenger / Parking",
    CATEGORIES.OTHER_UNCATEGORIZED: "Other, Uncategorized"
}

TYPES_BY_CATEGORY = {
    CATEGORIES.SURFACE_PARKING: ["Surface Parking Lot"],
    CATEGORIES.PARKING_STRUCTURE: ["Parking Structure"],
    CATEGORIES.TRANSIT_STATION: [
        "At-Grade Fixed Guideway Station", 
        "Simple At-Grade Platform Station", 
        "Bus Transfer Center", 
        "Elevated Fixed Guideway Station", 
        "Underground Fixed Guideway Station", 
        "Exclusive Platform Station", 
        "Ferryboat Terminal",
    ],
    CATEGORIES.ADMINISTRATIVE: ["Administrative Office / Sales Office", "Revenue Collection Facility"],
    CATEGORIES.MAINTENANCE: [
        "General Purpose Maintenance Facility / Depot",
        "Maintenance Facility (Service and Inspection)",
        "Vehicle Washing Facility",
        "Heavy Maintenance & Overhaul (Backshop)",
        "Vehicle Testing Facility",
        "Vehicle Blow-Down Facility"
    ],
    CATEGORIES.FUELING: ["Vehicle Fueling Facility"],
    CATEGORIES.OTHER_ADMIN_MAINTENANCE: [
        "Other, Administrative & Maintenance (describe in Notes)", 
        "Combined Administrative and Maintenance Facility (describe in Notes)",
    ],
    CATEGORIES.OTHER_PASSENGER_PARKING: ["Other, Passenger or Parking (describe in Notes)"],
    CATEGORIES.OTHER_UNCATEGORIZED: []
}

DEFAULT_CATEGORY = CATEGORIES.OTHER_UNCATEGORIZED