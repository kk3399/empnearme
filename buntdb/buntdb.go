package buntdb

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	domain "github.com/kdamarla/empnearme/domain"
	log "github.com/kdamarla/empnearme/log"

	"github.com/tidwall/buntdb"
)

//LcaRepo - data infrastructure
type LcaRepo struct {
	db              *buntdb.DB
	nameCacheDb     *buntdb.DB
	locationCacheDb *buntdb.DB
	log             log.Writer
}

//geoCoord type
type geoCoord struct {
	lat  float64
	long float64
}

type caseDistance struct {
	casen string
	dist  int
}

var zipcodeMap map[string]geoCoord

const zipcodemapFileName = "zipcodemap.csv"
const indexNearBy = "nearby"
const indexEmployerName = "searchemployer"
const lcaKeyPrefix = "lca"
const lcaPositionKeySuffix = "pos"
const lcaJSONKeySuffix = "json"
const lcaEmpNameKeySuffix = "empname"

const nameCacheDbFileName = "name_cache.db"
const locationCacheDbFileName = "location_cache.db"

//Init database
func Init(log log.Writer, filename string) LcaRepo {

	doesDBexist := true
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		doesDBexist = false
	}

	log.Info("opening db file")
	db, err := buntdb.Open(filename)
	//db, err := buntdb.Open(":memory:")
	if err != nil {
		log.Write(err)
	}

	log.Info("setting db config: ")
	var config buntdb.Config
	if err := db.ReadConfig(&config); err != nil {
		log.Fatal(err.Error())
	}

	config.AutoShrinkDisabled = true

	if err := db.SetConfig(config); err != nil {
		log.Fatal(err.Error())
	}

	log.Info("creating spatial index: ")
	err = db.CreateSpatialIndex(indexNearBy, fmt.Sprintf("%s:%s:%s", lcaKeyPrefix, "*", lcaPositionKeySuffix), buntdb.IndexRect)
	if err != nil {
		log.Write(err)
	}

	/*

		log.Info("creating index on employer name: ")
		err = db.CreateIndex(indexEmployerName, fmt.Sprintf("%s:%s:%s", lcaKeyPrefix, "*", lcaEmpNameKeySuffix), buntdb.IndexString)
		if err != nil {
			log.Write(err)
		}
	*/

	// Load cache database
	doesNameCacheDBexist, doesLocationCacheDBexist := true, true
	if _, err := os.Stat(nameCacheDbFileName); os.IsNotExist(err) {
		doesNameCacheDBexist = false
	}

	if _, err := os.Stat(locationCacheDbFileName); os.IsNotExist(err) {
		doesLocationCacheDBexist = false
	}

	/*
		log.Info("opening nameCacheDbFileName: ")
		nameCacheDb, err := buntdb.Open(nameCacheDbFileName)
		if err != nil {
			log.Write(err)
		}

		log.Info("setting nameCacheDbFileName db config: ")
		if err := nameCacheDb.ReadConfig(&config); err != nil {
			log.Fatal(err.Error())
		}

		config.AutoShrinkDisabled = true

		if err := nameCacheDb.SetConfig(config); err != nil {
			log.Fatal(err.Error())
		}
	*/

	log.Info("opening locationCacheDbFileName: ")
	locationCacheDb, err := buntdb.Open(locationCacheDbFileName)
	if err != nil {
		log.Write(err)
	}

	log.Info("setting locationCacheDbFileName db config: ")
	if err := locationCacheDb.ReadConfig(&config); err != nil {
		log.Fatal(err.Error())
	}

	config.AutoShrinkDisabled = true

	if err := locationCacheDb.SetConfig(config); err != nil {
		log.Fatal(err.Error())
	}

	log.Info("done initializing databases: ")
	lcaRepo := LcaRepo{db: db, log: log, locationCacheDb: locationCacheDb}
	if !doesDBexist {
		lcaRepo.load()
	}

	if !doesNameCacheDBexist {
		lcaRepo.compileEmpNameCache()
	}

	if !doesLocationCacheDBexist {
		lcaRepo.compileLocationCirclesCache()
	}

	return lcaRepo
}

//Close database connection
func (lcaRepo LcaRepo) Close() {
	lcaRepo.db.Close()
}

//Load loads all lca from flat files
func (lcaRepo LcaRepo) load() {
	for year := time.Now().Year(); year >= 2013; year-- {
		lcaRepo.loadYear(year)
	}
}

func (lcaRepo LcaRepo) compileLocationCirclesCache() {

	lcaRepo.log.Info("start compileLocationCirclesCache")

	w := runtime.NumCPU() / 2
	var wg sync.WaitGroup
	loadZipCodesIfNeeded()
	zips := make(chan string)

	go func() {
		for z := range zipcodeMap {
			zips <- z
		}
		<-time.After(time.Second) //needed?
		close(zips)
	}()

	wg.Add(w)
	for i := 0; i < w; i++ {
		go func() {
			for z := range zips {
				zipDistmap := make(map[string]string)
				r := 500
				caseDistances, err := lcaRepo.nearBy(zipcodeMap[z], r)
				if err == nil {
					for _, caseDist := range caseDistances {
						//add them to a map with proper key, then loop over map and insert to cahce db?
						zipDistKey := getZipDistanceKey(z, caseDist.dist)
						if val, ok := zipDistmap[zipDistKey]; ok {
							zipDistmap[zipDistKey] = val + "," + caseDist.casen
						} else {
							zipDistmap[zipDistKey] = caseDist.casen
						}
					}
				} else {
					lcaRepo.log.Write(err)
				}

				for zipDistKey, cases := range zipDistmap {
					err := lcaRepo.locationCacheDb.Update(func(tx *buntdb.Tx) error {
						_, _, err := tx.Set(zipDistKey, cases, nil)
						return err
					})
					if err != nil {
						lcaRepo.log.Write(err)
					}
				}
			}
			wg.Done()
		}()
	}

	wg.Wait()
	lcaRepo.log.Info("done compileLocationCircles")
}

func getZipDistanceKey(zip string, dist int) string {
	return zip + fmt.Sprintf("%03d", ((dist/5)+1)*5)
}

func (lcaRepo LcaRepo) compileEmpNameCache() {
	empNameMap := make(map[string]string)
	c := 0 //todo - delete me

	lcaRepo.log.Info("start compileEmpNameCache")
	lcaRepo.log.Info("	start compiling map of employer names")

	lcaRepo.db.View(func(tx *buntdb.Tx) error {
		tx.Ascend(indexEmployerName, func(casenum, empName string) bool {
			if len(empName) <= 0 {
				c++
				return true
			}

			if val, ok := empNameMap[empName]; ok {
				empNameMap[empName] = val + "," + casenum
			} else {
				empNameMap[empName] = casenum
			}

			return true
		})
		return nil
	})

	lcaRepo.log.Info(fmt.Sprintf("%d recods doesnt have employer name", c))
	lcaRepo.log.Info("done compiling map of employer names, starting adding to cache db")

	for empName, cases := range empNameMap {
		err := lcaRepo.nameCacheDb.Update(func(tx *buntdb.Tx) error {
			_, _, err := tx.Set(empName, cases, nil)
			return err
		})
		if err != nil {
			lcaRepo.log.Write(err)
		}
	}

	lcaRepo.log.Info("done compileEmpCase")
}

//Get lcas
func (lcaRepo LcaRepo) Get(searchCriteria domain.SearchCriteria) ([]domain.Lca, error) {

	var filterEmployer, filterPay, filterH1Date bool
	var lcas []domain.Lca

	if len(searchCriteria.Employer) > 0 {
		filterEmployer = true
	}

	if searchCriteria.MinimumPay > 0 {
		filterPay = true
	}

	if !searchCriteria.H1FiledAfter.IsZero() {
		filterH1Date = true
	}

	if len(searchCriteria.Zipcode) > 0 {
		if searchCriteria.Radius < 5 {
			searchCriteria.Radius = 5
		}

		var cases []string
		var err error
		for r := 5; r < searchCriteria.Radius; r = r + 5 {
			err = lcaRepo.locationCacheDb.View(func(tx *buntdb.Tx) error {
				val, err := tx.Get(searchCriteria.Zipcode + fmt.Sprintf("%03d", r))
				if err != nil {
					return err
				}
				cases = append(cases, strings.Split(val, ",")...)
				return nil
			})
		}

		if err != nil {
			geoCoord, err := getGeoCoordFromZip(searchCriteria.Zipcode)
			if err != nil {
				return nil, err
			}
			var caseDistances []caseDistance
			caseDistances, err = lcaRepo.nearBy(geoCoord, searchCriteria.Radius)
			if err != nil {
				return nil, err
			}

			for _, caseD := range caseDistances {
				cases = append(cases, caseD.casen)
			}
		}

		for _, casen := range cases {
			err := lcaRepo.db.View(func(tx *buntdb.Tx) error {
				//get the json key from position key
				jsonKey := getJSONKeyFromPositionKey(casen)
				//get the lca json
				value, err := tx.Get(jsonKey, true)
				//lcaRepo.log.Info("got data")
				if err == nil {
					var lca domain.Lca
					err := json.Unmarshal([]byte(value), &lca)
					if err == nil {
						if !((filterEmployer && !lca.EmployerNamed(searchCriteria.Employer)) ||
							(filterPay && !lca.PayMoreThan(searchCriteria.MinimumPay)) ||
							(filterH1Date && !lca.H1FiledAfter(searchCriteria.H1FiledAfter))) {
							lcas = append(lcas, lca)
						}
					} else {
						lcaRepo.log.Write(err)
					}
				} else {
					lcaRepo.log.Write(err)
				}
				return nil
			})
			if err != nil {
				lcaRepo.log.Write(err)
			}
		}
		return lcas, nil
	}

	if filterEmployer {
		var cases []string
		var err error

		lcaRepo.nameCacheDb.View(func(tx *buntdb.Tx) error {
			value, err := tx.Get(searchCriteria.Employer, true)
			if err == nil && len(value) > 0 {
				cases = strings.Split(value, ",")
			}
			return nil
		})

		if len(cases) == 0 {
			cases, err = lcaRepo.ofEmployer(searchCriteria.Employer)
			if err != nil {
				return nil, err
			}
		}

		for _, casenum := range cases {
			err := lcaRepo.db.View(func(tx *buntdb.Tx) error {
				//get the json key from position key
				jsonKey := getJSONKeyFromEmpNameKey(casenum)
				//get the lca json
				value, err := tx.Get(jsonKey, true)
				//lcaRepo.log.Info("got data")
				if err == nil {
					var lca domain.Lca
					err := json.Unmarshal([]byte(value), &lca)
					if err == nil {
						if !(filterPay && !lca.PayMoreThan(searchCriteria.MinimumPay)) ||
							(filterH1Date && !lca.H1FiledAfter(searchCriteria.H1FiledAfter)) {
							lcas = append(lcas, lca)
						}
					} else {
						lcaRepo.log.Write(err)
					}
				} else {
					lcaRepo.log.Write(err)
				}
				return nil
			})
			if err != nil {
				lcaRepo.log.Write(err)
			}
		}
		return lcas, nil
	}

	if filterPay {
		panic("not implemented yet")
	}

	if filterH1Date {
		panic("not implemented yet")
	}

	return nil, nil
}

func (lcaRepo LcaRepo) ofEmployer(employerName string) ([]string, error) {
	var cases []string
	err := lcaRepo.db.View(func(tx *buntdb.Tx) error {
		tx.Ascend(indexEmployerName, func(key, value string) bool {
			fmt.Printf("%s: %s\n", key, value)
			if strings.Contains(value, employerName) {
				cases = append(cases, key)
			}
			return false
		})
		return nil
	})

	if err != nil {
		return nil, err
	}

	return cases, nil
}

func (lcaRepo LcaRepo) nearBy(geoCoord geoCoord, radius int) ([]caseDistance, error) {
	var caseDistances []caseDistance

	locationString := getLocationString(geoCoord)
	err := lcaRepo.db.View(func(tx *buntdb.Tx) error {
		err := tx.Nearby(indexNearBy, locationString, func(key, value string, distance float64) bool {
			thisGeoCoord, err := getGeoCoord(value)
			if err == nil {
				miles := getDistance(geoCoord.lat, geoCoord.long, thisGeoCoord.lat, thisGeoCoord.long)
				radius := float64(radius)
				if miles <= radius {
					caseDistances = append(caseDistances, caseDistance{casen: key, dist: int(miles)})
					return true
				}
			} else {
				lcaRepo.log.Write(err)
			}
			return false
		})
		return err
	})

	if err != nil {
		return nil, err
	}

	return caseDistances, nil
}

func (lcaRepo LcaRepo) add(lca domain.Lca) error {

	err := lcaRepo.db.Update(func(tx *buntdb.Tx) error {
		geoCoord, err := getGeoCoordFromZip(lca.Employer_zip)
		if err != nil {
			return err
		}

		lcaJSON, err := json.Marshal(lca)
		if err != nil {
			return err
		}

		_, _, err = tx.Set(fmt.Sprintf("%s:%s:%s", lcaKeyPrefix, lca.Case_number, lcaPositionKeySuffix), getLocationString(geoCoord), nil)
		if err != nil {
			tx.Rollback()
			return err
		}

		_, _, err = tx.Set(fmt.Sprintf("%s:%s:%s", lcaKeyPrefix, lca.Case_number, lcaJSONKeySuffix), string(lcaJSON), nil)
		if err != nil {
			tx.Rollback()
			return err
		}

		_, _, err = tx.Set(fmt.Sprintf("%s:%s:%s", lcaKeyPrefix, lca.Case_number, lcaEmpNameKeySuffix), lca.Employer_name_lower, nil)
		if err != nil {
			tx.Rollback()
			return err
		}

		return nil
	})

	return err
}

func (lcaRepo LcaRepo) loadYear(year int) error {

	lcaRepo.log.Info(fmt.Sprintf("start: %d", year))
	fileName := "data/" + strconv.Itoa(year) + ".csv"
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
			lca.Employer_name_lower = strings.ToLower(lca.Employer_name)
			i = i + 1
			lca.Employer_address = strings.TrimSpace(line[i])
			i = i + 1
			lca.Employer_city = strings.TrimSpace(line[i])
			i = i + 1
			lca.Employer_state = strings.TrimSpace(line[i])
			i = i + 1
			lca.Employer_zip = padLeft(strings.TrimSpace(line[i]), "0", 5)
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

func padLeft(str, pad string, lenght int) string {
	for {
		str = pad + str
		if len(str) > lenght {
			return str[0:lenght]
		}
	}
}

func getPay(wage string, unit string) (int, error) {
	p := 0
	if wage != "" {
		if strings.ToLower(unit) == "year" {
			return strconv.Atoi(strings.Replace(strings.Split(strings.Split(wage, "-")[0], ".")[0], ",", "", 1))
		}

		return 0, errors.New("unknown unit - " + unit)
	}
	return p, nil
}

func getJSONKeyFromPositionKey(key string) string {
	return strings.Replace(key, lcaPositionKeySuffix, lcaJSONKeySuffix, 1)
}

func getJSONKeyFromEmpNameKey(key string) string {
	return strings.Replace(key, lcaEmpNameKeySuffix, lcaJSONKeySuffix, 1)
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
func getGeoCoordFromZip(zipcode string) (geoCoord, error) {

	if len(zipcode) != 5 {
		return geoCoord{}, errors.New("zipcode is 5 digits and is not valid")
	}

	loadZipCodesIfNeeded()

	zipGeoCoord := zipcodeMap[zipcode]
	if zipGeoCoord.lat == 0 && zipGeoCoord.long == 0 {
		return zipGeoCoord, errors.New("latitude, longitude not found for zipcode " + zipcode)
	}
	return zipGeoCoord, nil
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

	zipcodeMap = make(map[string]geoCoord)
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
		zipcodeMap[padLeft(strings.TrimSpace(line[0]), "0", 5)] = geoCoord{lat: lat, long: long}
	}
	return nil
}

//getGeoCoord from location string
func getGeoCoord(locationString string) (geoCoord, error) {
	locationString = strings.Replace(locationString, "[", "", 1)
	locationString = strings.Replace(locationString, "]", "", 1)
	locationArray := strings.Split(locationString, " ")
	lat, err := strconv.ParseFloat(locationArray[1], 64)
	if err != nil {
		return geoCoord{}, err
	}

	long, err := strconv.ParseFloat(locationArray[0], 64)
	if err != nil {
		return geoCoord{}, err
	}
	return geoCoord{lat: lat, long: long}, nil
}

//getLocationString from geoCoord
func getLocationString(geoCoord geoCoord) string {
	return fmt.Sprintf("[%f %f]", geoCoord.long, geoCoord.lat)
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
