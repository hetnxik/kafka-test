"""Configuration for CSV ETL pipeline."""

DUMP_DIR = "data"
OUTPUT_DIR = "output"
CHUNK_SIZE = 5000000  # Larger chunks for better throughput with vectorized ops

# Package name constants by category
_CUST = [
    "in.juspay.nammayatri",
    "in.juspay.jatrisaathi",
    "in.mobility.odishayatri",
    "in.mobility.keralasavaariconsumer",
]
_DRV = [
    "in.juspay.nammayatripartner",
    "in.juspay.jatrisaathidriver",
    "in.mobility.odishayatripartner",
    "in.mobility.manayatripartner",
    "net.openkochi.yatripartner",
    "in.mobility.keralasavaari",
]
_CHN = ["in.mobility.cumta"]
_BTC = ["in.mobility.bharatTaxi"]
_BTD = ["in.mobility.bharattaxidriver"]

# Mapping: event_name -> list of package_names it applies to
# Drives zero-fill only (ensures every combo appears in output even with 0 counts)
EVENT_PACKAGE_MAPPING = {
    # --- NY, OY, YS, KS Customer Events ---
    "ny_rider_ride_completed":          _CUST,
    "app_remove":                       _CUST + _DRV + _BTC + _BTD,
    "ny_user_ride_started":             _CUST,
    "ny_user_request_quotes":           _CUST + _BTC,
    "ny_user_source_and_destination":   _CUST + _BTC,
    "ny_user_ride_assigned":            _CUST,
    "ny_user_ride_completed":           _CUST,
    "first_open":                       _CUST,
    "ny_user_twostar_rating":           _CUST,
    "ny_user_onboarded":                _CUST + _BTC,
    "ny_cab_firstride":                 _CUST,
    # NY only
    "ny_user_first_ride_completed":     ["in.juspay.nammayatri"] + _BTC,
    "Metro_ticket_payment_successful":  ["in.juspay.nammayatri"] + _BTC,
    # OS only
    "Odishauser_first_ride_completed":  ["in.mobility.odishayatri"],
    # YS only
    "Yatriuser_first_ride_completed":   ["in.juspay.jatrisaathi"],
    "ys_bike_firstride":                ["in.juspay.jatrisaathi"],
    # KS only
    "keralaSavaariuser_first_ride_completed": ["in.mobility.keralasavaariconsumer"],
    # CO (Chennai One customer-side)
    "mt_home_train":                    _CHN,
    "mt_journey_info_pay":              _CHN,
    "mt_home_metro":                    _CHN,
    "mt_home_bus":                      _CHN,

    # --- NY, OY, YS, KS Driver Events ---
    "user_session_start":               _DRV + _BTD,
    "NEW_SIGNUP":                       _DRV + _BTD,
    "end_ride_success":                 _DRV + _BTD,
    "notification_open":                _DRV + _BTC + _BTD,
    "ny_user_app_version":              _CUST + _DRV + _CHN + _BTC + _BTD,
    "ny_driver_status_change":          _DRV + _BTD,
    "ride_cancelled":                   _DRV + _BTD,
    "FIRST_RIDE_COMPLETED":             _DRV + _BTD,

    # --- Chennai One ---
    "mt_home_search":                   _CHN,
    "NY_BUS_OTP_TYPED":                 _CHN,
    "NY_BUS_OTP_SCANNED":               _CHN,
    "MT_JOURNEY_INFO_PAY":              _CHN,
    "METRO_TICKET_PAYMENT_FAILED":      _CHN,
    "METRO_TICKET_PAYMENT_SUCCESSFUL":  _CHN,

    # --- Bharat Taxi Customer ---
    "ny_app_started":                   _CUST + _CHN + _BTC,
    "driver_assigned":                  _DRV + _BTD,
    "notification_recieve":             _BTC,
    "serviceTab_homeScreen":            _CUST + _CHN + _BTC,

    # --- Bharat Taxi Driver ---
    "btp_new_signup_del":               _BTD,
    "btp_new_signup_ahm":               _BTD,
    "btp_new_signup_noi":               _BTD,
    "btp_new_signup_gur":               _BTD,
    "btp_new_signup_sur":               _BTD,
    "btp_new_signup_vad":               _BTD,
}
