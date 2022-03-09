import requests
import json
from typing import Dict, Tuple

import validators
from corpus.datasources.openresearch import OREvent, OREventSeries
from flask import Blueprint, request, jsonify
from onlinespreadsheet.tableediting import TableEditing


class ValidationBlueprint(object):
    """
        Flask Blueprint providing routes to the profile pages
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
            self.template_folder = 'profile'
        self.blueprint = Blueprint(name, __name__, template_folder=self.template_folder, url_prefix="/validate")
        self.app = app
        self.appWrap = appWrap

        @self.blueprint.route('/homepage', methods=["POST"])
        @self.appWrap.csrf.exempt
        def validateHomepage():
            return self.validateHomepage()

        @self.blueprint.route('/ordinalFormat', methods=["POST"])
        @self.appWrap.csrf.exempt
        def validateOrdinalFormat():
            return self.validateOrdinalFormat()

        app.register_blueprint(self.blueprint)

    def validateHomepage(self):
        """
        validates the homepage

        Returns:
            validation result as json
        """
        tableEditing = self.getTableEdingFromRequest()
        res = HomepageValidator.validate(tableEditing)
        return jsonify(res)

    def validateOrdinalFormat(self):
        """
        validates the ordinal

        Returns:
            validation result as json
        """
        tableEditing = self.getTableEdingFromRequest()
        res = OrdinalValidator.validate(tableEditing)
        return jsonify(res)

    def getTableEdingFromRequest(self) -> TableEditing:
        lods = json.loads(request.json)
        tableEditing = TableEditing()
        tableEditing.lods=lods
        return tableEditing

class Validator(object):
    """
    Validates a dict of dicts
    """

    def validate(self, tableEditing:TableEditing) -> dict:
        """
        validates the given tableEditing
        Args:
            tableEditing: lods to be validated

        Returns:

        """
        return NotImplemented

    @classmethod
    def validateRecord(cls, entityName:str, entityType:str, entityRecord:dict) -> (bool, list):
        """
        validates the given entity
        Args:
            entityName: name of the entity
            entityType: type of the entity
            entityRecord: entity record

        Returns:
            isValid, list of error messages/ reasons of invalidation
        """
        return NotImplemented

    @classmethod
    def validateRecordBased(cls, tableEditing:TableEditing) -> dict:
        """
        validate each record individually by calling validateRecord
        Args:
            tableEditing: lods to be validated

        Returns:

        """
        validationResult = {}
        for tableName, lod in tableEditing.lods.items():
            validationResult[tableName] = {}
            for d in lod:
                pageTitle = d.get("pageTitle")
                isValid, errMsgs = cls.validateRecord(entityName=pageTitle, entityType=tableName, entityRecord=d)
                if isValid is None and errMsgs is None:
                    continue
                validationResult[tableName][pageTitle] = {"result": isValid, "errors": errMsgs}
        return validationResult


class HomepageValidator(Validator):
    """
    validates the homepage url
    """
    TIMEOUT = 2

    @classmethod
    def validate(cls, tableEditing:TableEditing) -> dict:
        return cls.validateRecordBased(tableEditing)

    @classmethod
    def validateRecord(cls, entityName:str, entityType:str, entityRecord:dict) -> (bool, list):
        if entityType == OREvent.templateName:
            mustContain = entityRecord.get("eventInSeries", None)
        elif entityType == OREventSeries.templateName:
            mustContain = entityRecord.get("acronym", None)
        else:
            mustContain = None
        homepage = entityRecord.get("homepage", None)
        if homepage is not None:
            isValid, errMsgs = cls.validateUrl(url=homepage, checkAvailability=True, mustContain=mustContain)
            return isValid, errMsgs
        return None, None

    @classmethod
    def validateUrl(cls, url:str, checkAvailability:bool=False, mustContain:str=None) -> (bool, list):
        """
        validates the availability of the given url
        Args:
            url: url to be validated

        Returns:
            (isAvailable, errMsg)
        """
        errMsg = []
        hasValidFormat = validators.url(url)
        if not hasValidFormat:
            errMsg.append("Url not in valid format")
        isAvailable = True
        contains = True
        if checkAvailability:
            try:
                resp = requests.get(url, allow_redirects=True, timeout=cls.TIMEOUT)
                isAvailable = resp.status_code == 200
            except Exception as e:
                isAvailable = False
            if isAvailable:
                if mustContain is not None:
                    contains = mustContain in resp.text
                    if not contains:
                        errMsg.append("Expected content not found")
            else:
                isArchived = cls.isArchivedUrl(url)
                msg = "Site not available"
                if isArchived:
                    msg += " (but archived)"
                errMsg.append(msg)
        isValid = all([hasValidFormat, isAvailable, contains])
        return isValid, errMsg

    @classmethod
    def isArchivedUrl(cls, url):
        """
        Checks whether the given url is archived or not
        see https://archive.org/help/wayback_api.php
        Args:
            url: url to be checked

        Returns:

        """
        try:
            archiveUrl = f"https://archive.org/wayback/available?url={url}"
            resp = requests.get(archiveUrl, timeout=cls.TIMEOUT)
            res = resp.json()
            isArchived = False
            if "archived_snapshots" in res:
                if "closest" in res["archived_snapshots"]:
                    isArchived = res["archived_snapshots"]["closest"].get("available", False)
            return isArchived
        except Exception as e:
            return False


class OrdinalValidator(Validator):
    """
    validates the ordinal of events
    """

    @classmethod
    def validate(cls, tableEditing:TableEditing) -> dict:
        return cls.validateRecordBased(tableEditing)

    @classmethod
    def validateRecord(cls, entityName:str, entityType:str, entityRecord:dict) -> (bool, list):
        if entityType != OREvent.templateName:
            return None, None
        hasValidFormat = None
        ordinal = entityRecord.get("ordinal", None)
        errMsgs = []
        if ordinal is None:
            errMsgs.append("Ordinal missing")
        else:
            hasValidFormat = cls.validateOrdinalFormat(ordinal)
            if not hasValidFormat:
                errMsgs.append("Ordinal format invalid")
        return hasValidFormat, errMsgs


    @classmethod
    def validateOrdinalFormat(cls, ordinal) -> bool:
        """
        Validates the given ordinal
        Args:
            ordinal: ordinal to be validated

        Returns:
            bool
        """
        if isinstance(ordinal, str):
            return ordinal.isnumeric()
        elif isinstance(ordinal, float):
            return ordinal.is_integer()
        elif isinstance(ordinal, int):
            return True
        else:
            return False

