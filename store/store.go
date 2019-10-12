package store

import (
	"encoding/csv"
	"encoding/gob"
	"errors"
	"fmt"
	"math"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	domain "github.com/kk3399/empnearme/domain"
	log "github.com/kk3399/empnearme/log"
)

//LcaRepo - data infrastructure
type LcaRepo struct {
	store store
	log   log.Writer
}

type store struct {
	Cases          map[string]domain.Lca
	EmployerCases  map[string][]string
	ZipcodeCases   map[int][]string
	ZipcodesNearBy map[int][]int
}

//geoCoord type
type geoCoord struct {
	lat   float64
	long  float64
	inUse bool
}

type caseDistance struct {
	casen string
	dist  int
}

const (
	constLcaResponseCap   = 5000
	constMaxRadiusInMiles = 51
)

var zipcodeMap map[int]*geoCoord

const zipcodemapFileName = "zipcodemap.csv"
const datastoreFilename = "data.gob"

//Init database
func Init(log log.Writer) LcaRepo {
	lcaRepo := LcaRepo{log: log}
	if _, err := os.Stat(datastoreFilename); os.IsNotExist(err) {
		log.Info("initializing databases: ")
		lcaRepo.store = store{
			Cases:          make(map[string]domain.Lca),
			EmployerCases:  make(map[string][]string),
			ZipcodeCases:   make(map[int][]string),
			ZipcodesNearBy: make(map[int][]int),
		}
		lcaRepo.loadStore()
		cleanTempMaps()
		//lcaRepo.save()
		//runtime.GC()
		log.Info("DONE initializing databases: ")
	} else {
		log.Info("opening db file")
		var dataStore = new(store)
		err = readGob(datastoreFilename, dataStore)
		if err != nil {
			log.Error(err.Error())
		} else {
			lcaRepo.store = *dataStore
		}
	}

	return lcaRepo
}

func cleanTempMaps() {
	zipcodeMap = make(map[int]*geoCoord)
}

//Close database connection
func (lcaRepo LcaRepo) save() {
	err := writeGob(datastoreFilename, lcaRepo.store)
	if err != nil {
		lcaRepo.log.Error(err.Error())
	}
}

func writeGob(filePath string, object interface{}) error {
	file, err := os.Create(filePath)
	if err == nil {
		encoder := gob.NewEncoder(file)
		encoder.Encode(object)
	}
	file.Close()
	return err
}

func readGob(filePath string, object interface{}) error {
	file, err := os.Open(filePath)
	if err == nil {
		decoder := gob.NewDecoder(file)
		err = decoder.Decode(object)
	}
	file.Close()
	return err
}

//Load loads all lca from flat files
func (lcaRepo LcaRepo) loadStore() {
	loadZipCodesIfNeeded()

	for year := time.Now().Year(); year >= 2013; year-- {
		lcaRepo.loadYear(year)
	}

	for zipcodeFrom := range zipcodeMap {
		geoCoordFrom, _ := getGeoCoordFromZip(zipcodeFrom)
		if geoCoordFrom.inUse == false {
			continue
		}
		for zipcodeTo := range zipcodeMap {
			geoCoordTo, _ := getGeoCoordFromZip(zipcodeTo)
			if geoCoordTo.inUse == false {
				continue
			}
			miles := getDistance(geoCoordFrom.lat, geoCoordFrom.long, geoCoordTo.lat, geoCoordTo.long)
			if miles < constMaxRadiusInMiles {
				iZipcodeFrom := (zipcodeFrom * 100) + int(miles/5)

				if val, ok := lcaRepo.store.ZipcodesNearBy[iZipcodeFrom]; ok {
					lcaRepo.store.ZipcodesNearBy[iZipcodeFrom] = append(val, zipcodeTo)
				} else {
					lcaRepo.store.ZipcodesNearBy[iZipcodeFrom] = []int{zipcodeTo}
				}
			}
		}
	}
}

//GetEmployerNames to return employe names for autocomplete
func (lcaRepo LcaRepo) GetEmployerNames(start string) map[string]int {
	r := make(map[string]int)
	for k := range lcaRepo.store.EmployerCases {
		if strings.Contains(k, start) {
			r[k] = 0
		}

	}
	return r
}

//Get lcas
func (lcaRepo LcaRepo) Get(searchCriteria domain.SearchCriteria) ([]domain.Lca, error) {

	var filterEmployer, filterPay, filterH1Year, excludeH1Dependent, filterJobTitle bool
	var lcas []domain.Lca

	if len(searchCriteria.Employer) > 0 {
		filterEmployer = true
	}

	if searchCriteria.PayMin > 0 && searchCriteria.PayMax > 0 {
		filterPay = true
	}

	if searchCriteria.H1Year > 0 {
		filterH1Year = true
	}

	if searchCriteria.ExcludeH1Dependent {
		excludeH1Dependent = true
	}

	if len(searchCriteria.JobTitle) > 0 {
		filterJobTitle = true
	}

	if len(searchCriteria.Zipcode) > 0 {

		searchCriteria.Zipcode = "1" + fmt.Sprintf("%05s", strings.TrimSpace(searchCriteria.Zipcode))

		if searchCriteria.Radius < 5 {
			searchCriteria.Radius = 5
		}

		var cases []string

		for i := 0; i < (searchCriteria.Radius/5)+1; i++ {
			zipkey, err := strconv.Atoi(searchCriteria.Zipcode + fmt.Sprintf("%02d", i))
			if err != nil {
				lcaRepo.log.Error(err.Error())
			}

			for _, casenum := range lcaRepo.store.ZipcodesNearBy[zipkey] {
				cases = append(cases, lcaRepo.store.ZipcodeCases[casenum]...)
			}

		}

		for _, casenum := range cases {
			if len(lcas) > constLcaResponseCap {
				break
			}
			lca := lcaRepo.store.Cases[casenum]
			if (!filterEmployer || lca.EmployerNamed(searchCriteria.Employer)) &&
				(!filterPay || lca.PayBetween(searchCriteria.PayMin, searchCriteria.PayMax)) &&
				(!filterH1Year || lca.Start_date.Year() == searchCriteria.H1Year) &&
				(!excludeH1Dependent || lca.H1b_dependent == "N") &&
				(!filterJobTitle || lca.HasJobTitle(searchCriteria.JobTitle)) {
				lcas = append(lcas, lca)
			}
		}
	}

	if filterEmployer {
		for _, casenum := range lcaRepo.store.EmployerCases[searchCriteria.Employer] {
			if len(lcas) > constLcaResponseCap {
				break
			}
			lca := lcaRepo.store.Cases[casenum]
			if (!filterPay || lca.PayBetween(searchCriteria.PayMin, searchCriteria.PayMax)) &&
				(!filterH1Year || lca.Start_date.Year() == searchCriteria.H1Year) &&
				(!excludeH1Dependent || lca.H1b_dependent == "N") &&
				(!filterJobTitle || lca.HasJobTitle(searchCriteria.JobTitle)) {
				lcas = append(lcas, lca)
			}
		}
	}

	if len(lcas) > constLcaResponseCap {
		return lcas[:constLcaResponseCap], nil
	}

	return lcas, nil
}

func (lcaRepo LcaRepo) add(lca domain.Lca) error {

	lcaRepo.store.Cases[lca.Case_number] = lca

	if val, ok := lcaRepo.store.EmployerCases[lca.Employer_name]; ok {
		lcaRepo.store.EmployerCases[lca.Employer_name] = append(val, lca.Case_number)
	} else {
		lcaRepo.store.EmployerCases[lca.Employer_name] = []string{lca.Case_number}
	}

	zipcodeKey, _ := strconv.Atoi(lca.Employer_zip)
	if val, ok := lcaRepo.store.ZipcodeCases[zipcodeKey]; ok {
		lcaRepo.store.ZipcodeCases[zipcodeKey] = append(val, lca.Case_number)
	} else {
		lcaRepo.store.ZipcodeCases[zipcodeKey] = []string{lca.Case_number}
	}

	if val, ok := zipcodeMap[zipcodeKey]; ok {
		val.inUse = true
	}

	return nil
}

func (lcaRepo LcaRepo) loadYear(year int) error {

	lcaRepo.log.Info(fmt.Sprintf("start: %d", year))
	fileName := path.Join("data", strconv.Itoa(year)+".csv")
	/*
		1-year	2-case_number	3-case_status	4-submit_date	5-decision_date	6-start_date	7-end_date	8-employer_name	9-employer_address
		10-employer_city	11-employer_state	12-employer_zip	13-job_title	14-naics_code	15-total_workers	16-full_time	17-wage_rate
		18-wage_unit	19-wage_level	20-h1b_dependent	21-willful_voilator	22-work_location_city	23-work_location_state	24-work_location_zip
	*/

	f, err := os.Open(fileName)
	if err != nil {
		return err
	}
	defer f.Close()

	// Read File into a Variable
	lines, err := csv.NewReader(f).ReadAll()
	if err != nil {
		panic(err)
	}
	dateLayout := "1/2/2006"
	dateAlternateLayout := "2/1/2006"

	// Loop through lines & turn into object
	for _, line := range lines {
		i := 0

		if line[i] != "year" {
			var lca = domain.Lca{}
			lca.Year, err = strconv.Atoi(strings.TrimSpace(line[i]))
			i = i + 1
			lca.Case_number = strings.TrimSpace(line[i])
			i = i + 1
			lca.Case_status = strings.TrimSpace(line[i])
			i = i + 1

			dt := strings.TrimSpace(line[i])
			if len(dt) > 0 {
				lca.Submit_date, err = time.Parse(dateLayout, dt)
				if err != nil {
					lca.Submit_date, err = time.Parse(dateAlternateLayout, dt)
					if err != nil {
						lcaRepo.log.Write(err)
					}
				}
			}
			i = i + 1

			dt = strings.TrimSpace(line[i])
			if len(dt) > 0 {
				lca.Decision_date, err = time.Parse(dateLayout, dt)
				if err != nil {
					lca.Decision_date, err = time.Parse(dateAlternateLayout, dt)
					if err != nil {
						lcaRepo.log.Write(err)
					}
				}
			}
			i = i + 1

			dt = strings.TrimSpace(line[i])
			if len(dt) > 0 {
				lca.Start_date, err = time.Parse(dateLayout, dt)
				if err != nil {
					lca.Start_date, err = time.Parse(dateAlternateLayout, dt)
					if err != nil {
						lcaRepo.log.Write(err)
					}
				}
			}
			i = i + 1

			dt = strings.TrimSpace(line[i])
			if len(dt) > 0 {
				lca.End_date, err = time.Parse(dateLayout, dt)
				if err != nil {
					lca.End_date, err = time.Parse(dateAlternateLayout, dt)
					if err != nil {
						lcaRepo.log.Write(err)
					}
				}
			}

			i = i + 1

			lca.Employer_name = strings.TrimSpace(line[i])
			i = i + 1
			lca.Employer_address = strings.TrimSpace(line[i])
			i = i + 1
			lca.Employer_city = strings.TrimSpace(line[i])
			i = i + 1
			lca.Employer_state = strings.TrimSpace(line[i])
			i = i + 1
			lca.Employer_zip = "1" + fmt.Sprintf("%05s", strings.TrimSpace(line[i]))
			i = i + 1
			lca.Job_title = strings.TrimSpace(line[i])
			i = i + 1
			lca.Soc_code = strings.TrimSpace(line[i])
			i = i + 1
			lca.Soc_name = strings.TrimSpace(line[i])
			i = i + 1
			lca.Naics_code = strings.TrimSpace(line[i])
			i = i + 1
			lca.Total_workers, err = strconv.Atoi(strings.TrimSpace(line[i]))
			i = i + 1
			lca.Full_time = strings.TrimSpace(line[i])
			i = i + 1
			lca.Wage_rate = strings.TrimSpace(line[i])
			i = i + 1
			lca.Wage_unit = strings.TrimSpace(line[i])
			i = i + 1
			lca.Pay, err = getPay(lca.Wage_rate, lca.Wage_unit)
			lca.Wage_level = strings.TrimSpace(line[i])
			i = i + 1
			lca.H1b_dependent = strings.TrimSpace(line[i])
			i = i + 1
			lca.Willful_voilator = strings.TrimSpace(line[i])
			i = i + 1
			lca.Work_location_city = strings.TrimSpace(line[i])
			i = i + 1
			lca.Work_location_state = strings.TrimSpace(line[i])
			i = i + 1
			lca.Work_location_zip = strings.TrimSpace(line[i])

			if err == nil {
				err = lcaRepo.add(lca)
			}
		}

	}
	lcaRepo.log.Info(fmt.Sprintf("end: %d", year))

	return nil
}

func getPay(wage string, unit string) (int, error) {
	p := 0
	if wage != "" {
		if strings.ToLower(unit) == "year" {
			return strconv.Atoi(strings.Replace(strings.Replace(strings.Split(strings.Split(wage, "-")[0], ".")[0], ",", "", 1), "$", "", 1))
		}

		return 0, errors.New("unknown unit - " + unit)
	}
	return p, nil
}

func loadZipCodesIfNeeded() {
	if zipcodeMap == nil {
		err := loadZipcodeMap()
		if err != nil {
			panic("error loading zipcode to lat,long csv file to a map")
		}
	}
}

//getGeoCoordFromZip returns the lat long from zipcode
func getGeoCoordFromZip(zipcode int) (geoCoord, error) {

	loadZipCodesIfNeeded()

	zipGeoCoord := zipcodeMap[zipcode]
	if zipGeoCoord.lat == 0 && zipGeoCoord.long == 0 {
		return *zipGeoCoord, errors.New("latitude, longitude not found for zipcode")
	}
	return *zipGeoCoord, nil
}

func loadZipcodeMap() error {
	f, err := os.Open(zipcodemapFileName)
	if err != nil {
		return err
	}
	defer f.Close()

	// Read File into a Variable
	lines, err := csv.NewReader(f).ReadAll()
	if err != nil {
		panic(err)
	}

	zipcodeMap = make(map[int]*geoCoord)
	// Loop through lines & turn into object
	for _, line := range lines {

		lat, err := strconv.ParseFloat(strings.TrimSpace(line[1]), 64)
		if err != nil {
			return err
		}

		long, err := strconv.ParseFloat(strings.TrimSpace(line[2]), 64)
		if err != nil {
			return err
		}

		iZipcode, _ := strconv.Atoi("1" + fmt.Sprintf("%05s", strings.TrimSpace(line[0])))

		zipcodeMap[iZipcode] = &geoCoord{lat: lat, long: long}
	}
	return nil
}

// haversin(θ) function
func hsin(theta float64) float64 {
	return math.Pow(math.Sin(theta/2), 2)
}

// Distance function returns the distance (in meters) between two points of
//     a given longitude and latitude relatively accurately (using a spherical
//     approximation of the Earth) through the Haversin Distance Formula for
//     great arc distance on a sphere with accuracy for small distances
//
// point coordinates are supplied in degrees and converted into rad. in the func
//
// distance returned is MILES!!!!!!
// http://en.wikipedia.org/wiki/Haversine_formula
func getDistance(lat1, lon1, lat2, lon2 float64) float64 {
	// convert to radians
	// must cast radius as float to multiply later
	var la1, lo1, la2, lo2, r float64
	la1 = lat1 * math.Pi / 180
	lo1 = lon1 * math.Pi / 180
	la2 = lat2 * math.Pi / 180
	lo2 = lon2 * math.Pi / 180

	r = 6378100 // Earth radius in METERS

	// calculate
	h := hsin(la2-la1) + math.Cos(la1)*math.Cos(la2)*hsin(lo2-lo1)

	return 2 * r * math.Asin(math.Sqrt(h)) * 0.00062137
}
