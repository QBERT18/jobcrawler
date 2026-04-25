package domain

// Location describes where a job is physically based.
// Latitude and Longitude are optional — populated only when
// the source provides geo-coordinates or after geocoding.
type Location struct {
	City       string
	State      string
	Country    string
	PostalCode string
	Latitude   *float64
	Longitude  *float64
}