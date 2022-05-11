from corpus.datasources.openresearch import OREvent
from flask import Blueprint, jsonify, request
from geograpy.locator import LocationContext, Region, Country, Location, City, Locator
from lodstorage.sql import SQLDB
from onlinespreadsheet.tableediting import TableEditing
from ormigrate.issue220_location import LocationFixer


class LocationServiceBlueprint(object):
    """
    api for the LocationService
    """

    def __init__(self, app, name: str, template_folder: str = None, appWrap=None):
        '''
        construct me

        Args:
            name(str): my name
            welcome(str): the welcome page
            template_folder(str): the template folder
        '''
        self.name = name
        if template_folder is not None:
            self.template_folder = template_folder
        else:
            self.template_folder = 'location'
        self.blueprint = Blueprint(name, __name__, template_folder=self.template_folder, url_prefix="/location")
        self.app = app
        self.appWrap = appWrap
        self.locationService = LocationService()

        @self.blueprint.route('/<country>', methods=["GET"])
        @self.appWrap.csrf.exempt
        def getCountry(country):
            return self.getCountry(country)

        @self.blueprint.route('/<country>/<region>', methods=["GET"])
        @self.appWrap.csrf.exempt
        def getRegion(country, region):
            return self.getRegion(country, region)

        @self.blueprint.route('/<country>/<region>/<city>', methods=["GET"])
        @self.appWrap.csrf.exempt
        def getCity(country, region, city):
            return self.getCity(country, region, city)

        @self.blueprint.route('/enhance', methods=["POST"])
        @self.appWrap.csrf.exempt
        def enhanceLocations():
            return self.enhanceLocations()

        app.register_blueprint(self.blueprint)

    def getCountry(self, country:str):
        """
        return country information
        """
        location = self.locationService.getCountry(country)
        if request.args.get('reduce', None) is not None:
            location = self.locationService.toOpenResearchFormat(location, "Country")
        return jsonify(location)

    def getRegion(self, country:str, region:str):
        """
        return region information
        """
        location = self.locationService.getRegion(country, region)
        if request.args.get('reduce', None) is not None:
            location = self.locationService.toOpenResearchFormat(location, "Region")
        return jsonify(location)

    def getCity(self, country:str, region:str, city:str):
        """
        return country information
        """
        location = self.locationService.getCity(country, region, city)
        if request.args.get('reduce', None) is not None:
            location = self.locationService.toOpenResearchFormat(location, "City")
        return jsonify(location)

    def enhanceLocations(self):
        """
        enhance the location of the given events
        """
        tableEditing = self.getTableEdingFromRequest()
        self.locationService.enhanceLocation(tableEditing)
        return jsonify(tableEditing.lods)

    def getTableEdingFromRequest(self) -> TableEditing:
        lods = request.json
        tableEditing = TableEditing()
        tableEditing.lods=lods
        return tableEditing


class LocationService:

    CITY = "city"
    REGION = "region"
    COUNTRY = "country"

    def __init__(self, debug:bool=False):
        self.debug=debug
        Locator.getInstance().downloadDB()
        self.locationContext = LocationContext.fromCache()

    def getSqlDb(self):
        """
        Returns:
            geograpy3 locations database
        """
        cacheFile = self.locationContext.cityManager.getCacheFile()
        db = SQLDB(cacheFile, debug=self.debug, errorDebug=self.debug)
        return db

    def guessLocation(self, name:str):
        pass

    def enhanceLocation(self, tableEditing:TableEditing):
        """
        tries enhancing the event locations to the normalized representations
        Args:
            tableEditing:

        Returns:

        """
        if OREvent.templateName in tableEditing.lods:
            for eventRecord in tableEditing.lods[OREvent.templateName]:
                self.fixLocationOfEventRecord(eventRecord)


    def toOpenResearchFormat(self, locationRecord:dict, locationType:str):
        """

        Args:
            location(dict): location record

        Returns:

        """
        if locationRecord is None:
            return {}
        location = {
            "name": locationRecord.get("name"),
            "wikidataid": locationRecord.get("wikidataid"),
            "coordinates": f"{locationRecord.get('lat', 0)}, {locationRecord.get('lon', 0)}"
        }
        if locationType == "City":
            location['locationKind'] = "City"
            location['level'] = 5
            location['partOf'] = locationRecord.get("regionIso", "").replace("-", "/")
        elif locationType == "Region":
            location['locationKind'] = "Region"
            location['level'] = 4
            location['partOf'] = locationRecord.get("countryIso", "")
        elif locationType == "Country":
            location['locationKind'] = "Country"
            location['level'] = 3
        return location

    def getLocationByOrName(self, name:str) -> dict:
        """
        get location information based on OR location name
        Args:
            name: name of the location

        Returns:
            dict
        """
        locParts = name.split("/")
        locType = None
        if len(locParts) == 1:
            record = self.getCountry(locParts[0])
            locType = "Country"
        elif len(locParts) == 2:
            record = self.getRegion(locParts[0], locParts[1])
            locType = "Region"
        elif len(locParts) == 3:
            record = self.getCity(locParts[0], locParts[1], locParts[2])
            locType = "City"
        else:
            return {}
        return self.toOpenResearchFormat(record, locType)

    def getCity(self, countryIso:str, regionIso:str, name:str):
        """

        Args:
            name: name of the city
            regionIso: iso code of the region without country
            countryIso: country iso code

        Returns:
            dict information about the city
        """
        query="""SELECT * 
        FROM CityLookup
        WHERE name = ?
        AND regionIso = ?
        """
        db = self.getSqlDb()
        qres = db.query(query, (name, f"{countryIso}-{regionIso}"))
        if qres:
            return qres[0]
        return None

    def getRegion(self, countryIso:str, regionIso:str):
        """

        Args:
            regionIso: iso code of the region without country
            countryIso: country iso code

        Returns:
            dict information about the region
        """
        query="""SELECT * 
        FROM RegionLookup
        WHERE iso = ?
        """
        db = self.getSqlDb()
        qres = db.query(query, (f"{countryIso}-{regionIso}",))
        if qres:
            return qres[0]
        return None

    def getCountry(self, countryIso:str=None):
        """

        Args:
            countryIso: country iso code

        Returns:
            dict information about the country
        """
        query="""SELECT * 
        FROM CountryLookup
        WHERE iso = ?
        """
        db = self.getSqlDb()
        qres = db.query(query, (countryIso,))
        if qres:
            return qres[0]
        return None

    def getMatchingLocation(self, city:str=None, region:str=None, country:str=None):
        """
        Uses geograpy3 to retrieve the best match for the given location information by ranking the results of geograpy
        Args:
            city:
            region:
            country:

        Returns:
            Location
        """
        foundLocations = self.locationContext.locateLocation(city, region, country)
        if foundLocations:
            #find best match
            rankedLocs=[]
            for loc in foundLocations:
                rank=0
                checkLocation=loc
                if isinstance(checkLocation, City):
                    if city in self.locationAlsoKnownAs(checkLocation):
                        rank+=3
                    checkLocation=checkLocation.region
                if isinstance(checkLocation, Region):
                    if region in self.locationAlsoKnownAs(checkLocation):
                        rank+=2
                    checkLocation=checkLocation.country
                if isinstance(checkLocation, Country):
                    if country in self.locationAlsoKnownAs(checkLocation):
                        rank+=1
                rankedLocs.append((loc,rank))
            # Postprocess ranking for known issues
            for i, t in enumerate(rankedLocs):
                loc, rank=t
                if "Brussels" in [city, region, country] and loc.wikidataid=='Q239': rank+=3
                rankedLocs[i]=(loc, rank)
            rankedLocs.sort(key=lambda x:x[1], reverse=True)
            maxRank=rankedLocs[0][1]
            bestMatches=[loc for loc, rank in rankedLocs if rank==maxRank]
            bestMatches.sort(key=lambda loc: float(loc.population) if hasattr(loc, "population") and getattr(loc, "population") else 0.0,reverse=True)
            return bestMatches[0]
        else:
            return None

    def locationAlsoKnownAs(self, location:Location) -> list:
        """
        Returns all labels of the given location
        """
        table = "CityLookup"
        if isinstance(location, Country): table="CountryLookup"
        elif isinstance(location, Region): table="RegionLookup"
        query = f"""
        SELECT label
        FROM { table }
        WHERE wikidataid == ?
        """
        db = self.getSqlDb()
        queryRes = db.query(query, (location.wikidataid, ))
        return [record.get('label') for record in queryRes]


    def lookupLocation(self, countryName:str=None, regionName:str=None, cityName:str=None):
        '''
        Uses geograpy3 to find locations matching the given information
        Args:
            countryName: name of the country
            regionName: name of the region
            cityName: name of the city

        Returns:
            List of locations that match the given location information
        '''
        locationCombination = f"{countryName},{regionName},{cityName}"
        eventPlaces = {
            'city':cityName,
            'region':regionName,
            'country':countryName
        }
        for prop, value in eventPlaces.items():
            # filter out known invalid and None values
            if value in ["Online", "None", "N/A"]:
                value = None
            # Add eventLocations to event Places
            if value:
                if value.startswith("Category:"):
                    value = value.replace("Category:", "")
                if '/' in value:
                    value=value.split('/')[-1]
            eventPlaces[prop]=value
        bestMatch = self.getMatchingLocation(**eventPlaces)
        return bestMatch

    def fixLocationOfEventRecord(self, event:dict, errors:dict=None):
        '''
        Args:
            event(dict): event records containing the location values that should be fixed
            errors(dict): dictonary containing the errors of the given event record → new errors are added to the dict
        '''
        if errors is None:
            errors = {}
        eventCity = event.get(self.CITY)
        eventRegion = event.get(self.REGION)
        eventCountry = event.get(self.COUNTRY)

        bestMatch=self.lookupLocation(eventCountry, eventRegion, eventCity)
        if isinstance(bestMatch, City):
            event[self.CITY]=LocationFixer.getPageTitle(bestMatch)
            event[self.REGION] = LocationFixer.getPageTitle(bestMatch.region)
            event[self.COUNTRY] = LocationFixer.getPageTitle(bestMatch.country)
            event[f"{self.CITY}WikidataId"] = bestMatch.wikidataid
            event[f"{self.REGION}WikidataId"] = bestMatch.region.wikidataid
            event[f"{self.COUNTRY}WikidataId"] = bestMatch.country.wikidataid
        elif isinstance(bestMatch, Region):
            #event[self.CITY] = None
            event[self.REGION] = LocationFixer.getPageTitle(bestMatch)
            event[self.COUNTRY] = LocationFixer.getPageTitle(bestMatch.country)
            event[f"{self.REGION}WikidataId"] = bestMatch.wikidataid
            event[f"{self.COUNTRY}WikidataId"] = bestMatch.country.wikidataid
            errors["city_unknown"] = f"Location information did not match any city"
        elif isinstance(bestMatch, Country):
            #event[self.CITY] = None
            #event[self.REGION] = None
            event[self.COUNTRY] = LocationFixer.getPageTitle(bestMatch)
            event[f"{self.COUNTRY}WikidataId"] = bestMatch.wikidataid
            errors["region_unknown"] = f"Location information did not match any region or city"
        else:
            errors["country_unknown"] = f"Location information did not match any location"
            #event[self.CITY] = None
            #event[self.REGION] = None
            #event[self.COUNTRY] = None
        return event, errors