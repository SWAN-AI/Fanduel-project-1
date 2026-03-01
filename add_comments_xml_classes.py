import xml.etree.ElementTree as ET

INPUT_OWL = "dd_enriched_ontology.owl"
OUTPUT_OWL = "dd_enriched_ontology_with_comments.owl"

NS = {
    "rdf":  "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
    "owl":  "http://www.w3.org/2002/07/owl#",
    "xml":  "http://www.w3.org/XML/1998/namespace",
}

for p, u in NS.items():
    if p != "xml":
        ET.register_namespace(p, u)

OWL_CLASS_URI = NS["owl"] + "Class"

def local_name(uri: str) -> str:
    if not uri:
        return ""
    if "#" in uri:
        return uri.rsplit("#", 1)[-1]
    return uri.rstrip("/").rsplit("/", 1)[-1]

def get_text(elem):
    return (elem.text or "").strip() if elem is not None else ""

def has_child(elem, qname):
    return elem.find(qname, NS) is not None

def is_class_description(desc: ET.Element) -> bool:
    """
    Treat as class if:
      - has rdf:type owl:Class
      - OR has rdfs:subClassOf (common for class hierarchy exports)
    """
    # rdf:type owl:Class
    for t in desc.findall("rdf:type", NS):
        res = t.attrib.get(f"{{{NS['rdf']}}}resource")
        if res == OWL_CLASS_URI:
            return True

    # inferred class if has subclass relationship
    if desc.find("rdfs:subClassOf", NS) is not None:
        return True

    return False

def generate_definition(label: str) -> str:
    """
    Simple template — consistent for assignment.
    You can upgrade these later with a dictionary for nicer wording.
    """
    # Optional: special-case a few top-level ones if you want:
    specials = {
        "Business concept": "A business-level entity or event represented in data.",
        "Financial concept": "A business concept related to financial activity represented in data.",
        "Auth & verification concept": "A business concept related to authentication, verification, and user security.",
        "Person / identity concept": "A business concept representing users, identity attributes, or profiles.",
        "Data asset": "An asset that represents stored, managed, or governed data.",
        "Governance asset": "A governance artifact (e.g., policy, rule, standard) used to manage data and compliance.",
        "Physical data asset": "A physical data object (e.g., dataset, table, file) that stores data.",
        "Semantic asset": "A semantic definition (e.g., business term, metric) that provides shared meaning for data.",
        "Process": "A repeatable operational or technical activity that produces, transforms, or manages data.",
    }
    if label in specials:
        return specials[label]

    return f"A {label} concept represented in the ontology."

def main():
    tree = ET.parse(INPUT_OWL)
    root = tree.getroot()

    descs = root.findall(".//rdf:Description", NS)

    class_descs = []
    for d in descs:
        if is_class_description(d):
            class_descs.append(d)

    print(f"rdf:Description blocks: {len(descs)}")
    print(f"Class blocks detected: {len(class_descs)}")

    added = 0
    skipped_existing = 0
    skipped_no_id = 0

    for d in class_descs:
        about = d.attrib.get(f"{{{NS['rdf']}}}about") or d.attrib.get(f"{{{NS['rdf']}}}ID")
        if not about:
            skipped_no_id += 1
            continue

        # Skip if rdfs:comment already exists
        if d.find("rdfs:comment", NS) is not None:
            skipped_existing += 1
            continue

        label_elem = d.find("rdfs:label", NS)
        label = get_text(label_elem)
        if not label:
            label = local_name(about)

        definition = generate_definition(label)

        c = ET.Element(f"{{{NS['rdfs']}}}comment")
        c.set(f"{{{NS['xml']}}}lang", "en")
        c.text = definition

        # Insert after label if possible (looks nicer)
        if label_elem is not None:
            idx = list(d).index(label_elem)
            d.insert(idx + 1, c)
        else:
            d.append(c)

        added += 1

    tree.write(OUTPUT_OWL, encoding="utf-8", xml_declaration=True)

    print("Done.")
    print(f"Comments added: {added}")
    print(f"Skipped (already had comment): {skipped_existing}")
    print(f"Skipped (no rdf:about/ID): {skipped_no_id}")
    print(f"Saved: {OUTPUT_OWL}")

if __name__ == "__main__":
    main()