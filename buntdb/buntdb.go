package buntdb

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	domain "github.com/kdamarla/empnearme/domain"
	log "github.com/kdamarla/empnearme/log"

	"github.com/tidwall/buntdb"
)

//LcaRepo - data infrastructure
type LcaRepo struct {
	db  *buntdb.DB
	log log.Writer
}

//geoCoord type
type geoCoord struct {
	lat  float64
	long float64
}

var zipcodeMap map[string]geoCoord

const zipcodemapFileName = "zipcodemap.csv"
const indexNearBy = "nearby"
const indexPay = "pay"
const lcaKeyPrefix = "lca"
const lcaPositionKeySuffix = "pos"
const lcaJSONKeySuffix = "json"

//Init database
func Init(log log.Writer) LcaRepo {

	//db, err := buntdb.Open("data.db")
	db, err := buntdb.Open(":memory:")
	if err != nil {
		log.Write(err)
	}

	err = db.CreateSpatialIndex(indexNearBy, fmt.Sprintf("%s:%s:%s", lcaKeyPrefix, "*", lcaPositionKeySuffix), buntdb.IndexRect)
	if err != nil {
		log.Write(err)
	}

	err = db.CreateIndex(indexPay, fmt.Sprintf("%s:%s:%s", lcaKeyPrefix, "*", lcaJSONKeySuffix), buntdb.IndexJSON("wage_rate"))
	if err != nil {
		log.Write(err)
	}

	//defer db.Close()
	return LcaRepo{db: db, log: log}
}

//Close database connection
func (lcaRepo LcaRepo) Close() {
	lcaRepo.db.Close()
}

//Load loads all lca from flat files
func (lcaRepo LcaRepo) Load() {
	year := 2019
	for ; year < 2020; year++ {
		lcaRepo.loadYear(year)
	}
}

//Get lcas
func (lcaRepo LcaRepo) Get(locationFilter domain.Filter) ([]domain.Lca, error) {

	var lcas []domain.Lca

	if locationFilter.Radius > 0 && len(locationFilter.Zipcode) > 0 {
		geoCoord, err := getGeoCoordFromZip(locationFilter.Zipcode)
		if err != nil {
			return nil, err
		}

		locationString := getLocationString(geoCoord)
		err = lcaRepo.db.View(func(tx *buntdb.Tx) error {
			err := tx.Nearby(indexNearBy, locationString, func(key, value string, distance float64) bool {
				thisGeoCoord, err := getGeoCoord(value)
				if err == nil {
					miles := getDistance(geoCoord.lat, geoCoord.long, thisGeoCoord.lat, thisGeoCoord.long)
					radius := float64(locationFilter.Radius)
					if miles <= radius {
						//get the lca vin from key
						jsonKey := getJSONKeyFromPositionKey(key)
						//get the lca json
						value, err = tx.Get(jsonKey, false)
						if err == nil {
							var lca domain.Lca
							err := json.Unmarshal([]byte(value), &lca)
							if err == nil {
								lcas = append(lcas, lca)
							} else {
								lcaRepo.log.Write(err)
							}
						} else {
							lcaRepo.log.Write(err)
						}

						return true
					}
				} else {
					lcaRepo.log.Write(err)
				}
				return false
			})
			return err
		})
	}
	return lcas, nil
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

		return nil
	})

	return err
}

func (lcaRepo LcaRepo) loadYear(year int) error {
	fileName := "data/" + strconv.Itoa(year) + ".csv"
	/*
		1-year	2-case_number	3-case_status	4-submit_date	5-decision_date	6-start_date	7-end_date	8-employer_name	9-employer_address
		10-employer_city	11-employer_state	12-employer_zip	13-job_title	14-soc_code	15-soc_name	16-naics_code	17-total_workers
		18-full_time	19-wage_rate	20-wage_unit	21-wage_level	22-prevailing_wage_source	23-other_wage_source	24-prevailing_wage_from
		25-prevailing_wage_to	26-prevailing_wage_unit	27-h1b_dependent	28-willful_voilator	29-work_location_city	30-work_location_state
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
					panic(err)
				}
			}
			i = i + 1

			dt = strings.TrimSpace(line[i])
			if len(dt) > 0 {
				lca.Decision_date, err = time.Parse(dateLayout, dt)
				if err != nil {
					panic(err)
				}
			}
			i = i + 1

			dt = strings.TrimSpace(line[i])
			if len(dt) > 0 {
				lca.Start_date, err = time.Parse(dateLayout, dt)
				if err != nil {
					panic(err)
				}
			}
			i = i + 1

			dt = strings.TrimSpace(line[i])
			if len(dt) > 0 {
				lca.End_date, err = time.Parse(dateLayout, dt)
				if err != nil {
					panic(err)
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
			lca.Employer_zip = strings.TrimSpace(line[i])
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
			lca.Prevailing_wage_source = strings.TrimSpace(line[i])
			i = i + 1
			lca.Other_wage_source = strings.TrimSpace(line[i])
			i = i + 1
			lca.Prevailing_wage_from = strings.TrimSpace(line[i])
			i = i + 1
			lca.Prevailing_wage_to = strings.TrimSpace(line[i])
			i = i + 1
			lca.Prevailing_wage_unit = strings.TrimSpace(line[i])
			i = i + 1
			lca.H1b_dependent = strings.TrimSpace(line[i])
			i = i + 1
			lca.Willful_voilator = strings.TrimSpace(line[i])
			i = i + 1
			lca.Work_location_city = strings.TrimSpace(line[i])
			i = i + 1
			lca.Work_location_state = strings.TrimSpace(line[i])
			i = i + 1

			if err == nil {
				err = lcaRepo.add(lca)
			}
		}

	}
	return nil
}

func getPay(wage string, unit string) (int, error) {
	p := 0
	if wage != "" {
		if strings.ToLower(unit) == "year" {
			return strconv.Atoi(strings.Replace(strings.Split(wage, ".")[0], ",", "", 1))
		}

		return 0, errors.New("unknown unit - " + unit)
	}
	return p, nil
}

func getJSONKeyFromPositionKey(key string) string {
	return strings.Replace(key, lcaPositionKeySuffix, lcaJSONKeySuffix, 1)
}

//getGeoCoordFromZip returns the lat long from zipcode
func getGeoCoordFromZip(zipcode string) (geoCoord, error) {

	if len(zipcode) != 5 {
		return geoCoord{}, errors.New("zipcode is 5 digits and is not valid")
	}
	if zipcodeMap == nil {
		err := loadZipcodeMap()
		if err != nil {
			return geoCoord{}, fmt.Errorf(err.Error(), "error loading zipcode to lat,long csv file to a map")
		}
	}

	zipGeoCoord := zipcodeMap[zipcode]
	if zipGeoCoord.lat == 0 && zipGeoCoord.long == 0 {
		return zipGeoCoord, errors.New("latitude, longitude not found for zipcode")
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
		zipcodeMap[strings.TrimSpace(line[0])] = geoCoord{lat: lat, long: long}
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
