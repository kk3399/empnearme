package domain

import (
	"time"
)

// Lca info
type Lca struct {
	Year                int
	Case_number         string
	Case_status         string
	Submit_date         time.Time
	Decision_date       time.Time
	Start_date          time.Time
	End_date            time.Time
	Employer_name       string
	Employer_address    string
	Employer_name_lower string
	Employer_city       string
	Employer_state      string
	Employer_zip        string
	Job_title           string
	Soc_code            string
	Soc_name            string
	Naics_code          string
	Total_workers       int
	Full_time           string
	Wage_rate           string
	Wage_unit           string
	Wage_level          string
	Pay                 int
	H1b_dependent       string
	Willful_voilator    string
	Work_location_city  string
	Work_location_state string
	Work_location_zip   string
}

//LcaRepo handles read/write to database
type LcaRepo interface {
	Get(searchCriteria SearchCriteria) ([]Lca, error)
}

//SearchCriteria for search
type SearchCriteria struct {
	Radius             int
	Zipcode            string
	Employer           string
	MinimumPay         int
	ExcludeH1Dependent bool
	H1FiledAfter       time.Time
}

func (lca Lca) PayMoreThan(pay int) bool {
	return lca.Pay > pay
}

func (lca Lca) H1FiledAfter(after time.Time) bool {
	return lca.Submit_date.After(after)
}

func (lca Lca) EmployerNamed(employer string) bool {
	return lca.Employer_name_lower == employer
}
