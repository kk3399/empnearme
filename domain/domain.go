package domain

import (
	"strings"
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
	GetEmployerNames(has string) map[string]int
}

//SearchCriteria for search
type SearchCriteria struct {
	Radius             int
	Zipcode            string
	Employer           string
	PayMin             int
	PayMax             int
	ExcludeH1Dependent bool
	H1Year             int
	JobTitle           string
}

func (lca Lca) PayBetween(min int, max int) bool {
	return min <= lca.Pay && lca.Pay <= max
}

func (lca Lca) H1FiledAfter(after time.Time) bool {
	return lca.Submit_date.After(after)
}

func (lca Lca) EmployerNamed(employer string) bool {
	return lca.Employer_name == employer
}

func (lca Lca) HasJobTitle(jobTile string) bool {
	return strings.Contains(lca.Job_title, jobTile)
}
