from pathlib import Path
import json
import xml.etree.ElementTree as ET

import requests

from ncbi_utils.cache import hash_from_tuple, load_cache, save_cache

NCBI_EUTILS_BASE_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"


def _get_cached_request(url, cache_dir=None):
    if cache_dir is None:
        use_cache = False
    else:
        use_cache = True

    if use_cache:
        hash = hash_from_tuple((url,))
        cache_path = cache_dir / f"cached_request.{hash}.pickle"

    if use_cache and cache_path.exists():
        content = load_cache(cache_path)
    else:
        response = requests.get(url)
        assert response.status_code == 200
        content = response.content
        if use_cache:
            cache_dir.mkdir(exist_ok=True)
            save_cache(content, cache_path=cache_path)
    return content


def _fetch_bioproject_info_with_id(bioproject_id: str, cache_dir=None):
    if bioproject_id.lower().startswith("prj"):
        raise ValueError(
            f"Use the numeric id, not the PRJXXXX accession: {bioproject_id}"
        )
    try:
        int(bioproject_id)
    except ValueError:
        raise ValueError(
            f"The id should be an str, but all numbers (e.g. 1025377), but it was: {bioproject_id}"
        )

    query = f"{NCBI_EUTILS_BASE_URL}efetch.fcgi?db=bioproject&id={bioproject_id}"
    xml = _get_cached_request(query, cache_dir=cache_dir)
    xml_doc = ET.fromstring(xml)
    project_xml = xml_doc.find("DocumentSummary").find("Project")
    archive_id_tag = project_xml.find("ProjectID").find("ArchiveID")
    id = archive_id_tag.attrib["id"]
    assert id == bioproject_id
    bioproject = {"accession": archive_id_tag.attrib["accession"], "id": id}

    try:
        bioproject["name"] = project_xml.find("ProjectDescr").find("Name").text
    except AttributeError:
        pass
    bioproject["title"] = project_xml.find("ProjectDescr").find("Title").text
    bioproject["description"] = (
        project_xml.find("ProjectDescr").find("Description").text
    )
    return bioproject


def fetch_bioproject_info(bioproject_acc: str, cache_dir=None) -> dict:
    query = f"{NCBI_EUTILS_BASE_URL}esearch.fcgi?db=bioproject&term={bioproject_acc}[Project%20Accession]&retmode=json&retmax=1"
    jsons = _get_cached_request(query, cache_dir=cache_dir)
    jsons = jsons.decode()
    ncbi_data = json.loads(jsons)
    search_result = ncbi_data["esearchresult"]
    bioproject_id = search_result["idlist"][0]

    bioproject = _fetch_bioproject_info_with_id(bioproject_id)
    assert bioproject["accession"] == bioproject_acc
    return bioproject


def ask_ncbi_for_biosample_ids_in_bioproject(
    bioproject_id: str, cache_dir=None
) -> list[str]:
    if bioproject_id.lower().startswith("prj"):
        raise ValueError(
            f"Use the numeric id, not the PRJXXXX accession: {bioproject_id}"
        )
    try:
        int(bioproject_id)
    except ValueError:
        raise ValueError(
            f"The id should be an str, but all numbers (e.g. 1025377), but it was: {bioproject_id}"
        )
    query = f"{NCBI_EUTILS_BASE_URL}elink.fcgi?dbfrom=bioproject&db=biosample&id={bioproject_id}&retmode=json"
    jsons = _get_cached_request(query, cache_dir=cache_dir)
    search_result = json.loads(jsons)

    biosample_ids = set()
    for linkset in search_result["linksets"]:
        for linksetdb in linkset["linksetdbs"]:
            biosample_ids.update(linksetdb["links"])
    return sorted(biosample_ids)


def generate_fastq_dump_cmd(sra_run_acc, out_dir: Path):
    # sra_run_id example = SRR000001
    cmd = [
        "fastq-dump",
        "--split-3",
        "--skip-technical",
        "--gzip",
        "--defline-qual",
        "+",
        "--defline-seq",
        "@$ac.$si/$ri $sn",
        "--outdir",
        str(out_dir),
        sra_run_acc,
    ]
    return cmd


def fetch_biosample_info_with_id(biosample_id: str, cache_dir=None):
    if biosample_id.lower().startswith("prj"):
        raise ValueError(
            f"Use the numeric id, not the SAMNXXXX accession: {biosample_id}"
        )
    try:
        int(biosample_id)
    except ValueError:
        raise ValueError(
            f"The id should be an str, but all numbers (e.g. 1025377), but it was: {biosample_id}"
        )

    query = f"{NCBI_EUTILS_BASE_URL}efetch.fcgi?db=biosample&id={biosample_id}"
    xml = _get_cached_request(query, cache_dir=cache_dir)
    biosample_set = ET.fromstring(xml)
    biosample_xml = biosample_set.find("BioSample")
    biosample = {}
    biosample["biosampledb_id"] = biosample_xml.attrib["id"]
    biosample["biosampledb_accession"] = biosample_xml.attrib["accession"]
    biosample["publication_date"] = biosample_xml.attrib["publication_date"]
    for id in biosample_xml.find("Ids").findall("Id"):
        try:
            id.attrib["db"]
        except KeyError:
            continue
        if id.attrib["db"] == "SRA":
            biosample["sra_accession"] = id.text

    description = biosample_xml.find("Description")
    biosample["title"] = description.find("Title").text
    organism = description.find("Organism")
    biosample["organism_id"] = organism.attrib["taxonomy_id"]
    biosample["organism_name"] = organism.attrib["taxonomy_name"]

    attributes = {}
    for attribute_xml in biosample_xml.find("Attributes"):
        attributes[attribute_xml.attrib["attribute_name"]] = attribute_xml.text
    biosample["attributes"] = attributes

    return biosample


def _get_data_from_sra_experiment_package(experiment_package):
    experiment = {}
    xml = experiment_package.find("EXPERIMENT")
    experiment["accession"] = xml.attrib["accession"]
    experiment["title"] = xml.find("TITLE").text

    design = xml.find("DESIGN")
    experiment["design"] = {}
    experiment["design"]["description"] = design.find("DESIGN_DESCRIPTION").text
    experiment["design"]["biosample_sra_accession"] = design.find(
        "SAMPLE_DESCRIPTOR"
    ).attrib["accession"]
    experiment["design"]["library"] = {}
    library = design.find("LIBRARY_DESCRIPTOR")
    experiment["design"]["library"]["name"] = library.find("LIBRARY_NAME").text
    experiment["design"]["library"]["strategy"] = library.find("LIBRARY_STRATEGY").text
    experiment["design"]["library"]["source"] = library.find("LIBRARY_SOURCE").text
    experiment["design"]["library"]["selection"] = library.find(
        "LIBRARY_SELECTION"
    ).text
    experiment["design"]["library"]["layout"] = next(
        iter(library.find("LIBRARY_LAYOUT"))
    ).tag

    platform = xml.find("PLATFORM")
    experiment["platform"] = next(iter(platform)).tag

    pool = experiment_package.find("Pool")
    experiment["accessions"] = []
    for member in pool.findall("Member"):
        experiment["accessions"].append(member.attrib["accession"])

    runs = []
    for run in experiment_package.find("RUN_SET").findall("RUN"):
        run_info = {"accession": run.attrib["accession"]}
        run_info["num_spots"] = run.attrib["total_spots"]
        runs.append(run_info)

    experiment["runs"] = runs
    return experiment


def fetch_sra_info(sra_id, cache_dir=None):
    if sra_id.lower().startswith("prj"):
        raise ValueError(f"Use the numeric id, not the SAMNXXXX accession: {sra_id}")
    try:
        int(sra_id)
    except ValueError:
        raise ValueError(
            f"The id should be an str, but all numbers (e.g. 1025377), but it was: {sra_id}"
        )

    query = f"{NCBI_EUTILS_BASE_URL}efetch.fcgi?db=sra&id={sra_id}"
    xml = _get_cached_request(query, cache_dir=cache_dir)
    xml = ET.fromstring(xml)

    if xml.tag != "EXPERIMENT_PACKAGE_SET":
        raise RuntimeError("We were expecting an EXPERIMENT_PACKAGE_SET for the SRA id")

    experiments = []
    for experiment_package in xml.findall("EXPERIMENT_PACKAGE"):
        experiments.append(_get_data_from_sra_experiment_package(experiment_package))

    return {"experiments": experiments}


def search_experiments_in_sra_with_biosample_accession(biosample_acc, cache_dir=None):
    query = f"{NCBI_EUTILS_BASE_URL}esearch.fcgi?db=sra&term={biosample_acc}[BioSample]&retmode=json"
    jsons = _get_cached_request(query, cache_dir=cache_dir)
    search_result = json.loads(jsons)
    ids = search_result["esearchresult"]["idlist"]

    experiments = []
    for id in ids:
        experiments.extend(fetch_sra_info(id)["experiments"])
    return experiments


if __name__ == "__main__":
    if False:
        bioproject = fetch_bioproject_info(bioproject_acc="PRJNA961747")
    else:
        bioproject = {
            "accession": "PRJNA961747",
            "id": "961747",
            "title": "Coprolite metagenomes Huecoid and Saladoid Puerto Rico",
            "description": "Datasets contain metagenomic sequence data from pooled coprolite samples from the Huecoid and Saladoid cultures. These are extinct cultures that inhabit Puerto Rico with differences in culture and diet that are reflected in their coprolites.",
        }

    if False:
        biosample_ids = ask_ncbi_for_biosample_ids_in_bioproject(bioproject["id"])
        print(biosample_ids)
    else:
        biosample_ids = ["34367738", "34367739"]

    if False:
        biosample = fetch_biosample_info_with_id("34367739")
        print(biosample)
    else:
        biosample = {
            "biosampledb_id": "34367739",
            "biosampledb_accession": "SAMN34367739",
            "publication_date": "2023-04-25T00:00:00.000",
            "sra_accession": "SRS17427263",
            "title": "Saladoid coprolite sample",
            "organism_id": "749906",
            "organism_name": "gut metagenome",
            "attributes": {
                "collection_date": "2012",
                "env_broad_scale": "human-gut",
                "env_local_scale": "coprolite",
                "env_medium": "core",
                "geo_loc_name": "Puerto Rico",
                "host": "Homo sapiens",
                "lat_lon": "18.00 N 65.00 W",
                "Culture": "Saladoid",
            },
        }

    if True:
        experiments = search_experiments_in_sra_with_biosample_accession("SAMN34367739")
        print(experiments)
