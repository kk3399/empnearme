package domain

import (
	"time"
)

// Lca info
type Lca struct {
	Year                   int
	Case_number            string
	Case_status            string
	Submit_date            time.Time
	Decision_date          time.Time
	Start_date             time.Time
	End_date               time.Time
	Employer_name          string
	Employer_address       string
	Employer_city          string
	Employer_state         string
	Employer_zip           string
	Job_title              string
	Soc_code               string
	Soc_name               string
	Naics_code             string
	Total_workers          int
	Full_time              string
	Wage_rate              string
	Wage_unit              string
	Wage_level             string
	Pay                    int
	Prevailing_wage_source string
	Other_wage_source      string
	Prevailing_wage_from   string
	Prevailing_wage_to     string
	Prevailing_wage_unit   string
	H1b_dependent          string
	Willful_voilator       string
	Work_location_city     string
	Work_location_state    string
}

//LcaRepo handles read/write to database
type LcaRepo interface {
	Get(filter Filter) ([]Lca, error)
	Load(year int) error
}

//Filter for search
type Filter struct {
	Radius             int
	Zipcode            string
	Employer           string
	PayFrom            int
	PayTo              int
	ExcludeH1Dependent bool
	H1FiledAfter       time.Time
}
