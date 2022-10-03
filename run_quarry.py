"""Code for running QUARRY on an existing Neo4j graph.
@author Michael Stewart
"""

from efficient_apriori import apriori

import py2neo
from py2neo import Graph
from py2neo.data import Node, Relationship

# Ignore uncategorised items and FLOCs in the sample graph.
IGNORED_LABELS = set(["Item/Uncategorised", "FLOC"])


def rule_to_string(rule):
    """Convert a rule to a string representation.

    Args:
        rule (Object): The rule to convert.

    Returns:
        str: A string representation of the rule.
    """
    return f"{', '.join(rule.lhs)} -> {', '.join(rule.rhs)}"


def rule_to_short_string(rule):
    """Same as rule_to_string, but shorter.

    Args:
        rule (Object): The rule to convert.

    Returns:
        str: A short string representation of the rule.
    """
    return (
        f"{', '.join([x.split('/')[-1] for x in rule.lhs])}"
        f"-> {', '.join([x.split('/')[-1] for x in rule.rhs])}"
    )


def build_assocation_rule_graph(rules):
    """Construct the association rule graph from the list of rules.
    This creates Q_Rule entities which are linked to the Q_Entities appearing
    in those rules, which in turn are linked to the Q_Documents in which they
    appear.

    Args:
        rules (TYPE): Description
    """
    print("Building association rule graph")
    graph = Graph(password="password")
    tx = graph.begin()

    # Delete all rules and start over
    cursor = graph.run("MATCH (q:Q_Rule) DETACH DELETE(q)")

    for i, rule in enumerate(rules):
        r_node = Node(
            "Q_Rule",
            confidence=rule.confidence,
            support=rule.support,
            lift=rule.lift,
            rule_id=i,
            name=rule_to_string(rule),
            short_name=rule_to_short_string(rule),
        )
        graph.merge(r_node, "Q_Rule", "rule_id")
        for e in rule.lhs:
            graph.run(
                f"MATCH (e:Q_Entity {{name: '{e}'}}),"
                f"(r: Q_Rule {{rule_id: {i}}})"
                "MERGE (e)-[:ANTE]->(r)"
            )
        for e in rule.rhs:
            graph.run(
                f"MATCH (e:Q_Entity {{name: '{e}'}}),"
                f"(r: Q_Rule {{rule_id: {i}}})"
                "MERGE (e)-[:CONS]->(r)"
            )
    print(f"Created {len(rules)} rules in graph.")


def build_transactions_graph(documents, leaf_entities, entity_parents):
    """Build the transactions graph.
    In the current version of the code, we create a separate transaction
    graph of Q_Documents, Q_Entities and Q_Rules, rather than join the Q_Rules
    directly to the existing entities and documents. In an upcoming version
    the graphs will be merged together.

    Args:
        documents (list): The list of documents.
        leaf_entities (dict): A dict mapping leaf entities to the docs in which
          they appear.
        entity_parents (dict): A dict mapping entities to their parents.
    """
    print("Building transactions graph")
    graph = Graph(password="password")
    tx = graph.begin()

    total_leafs = 0
    for e, p in entity_parents.items():
        if e not in entity_parents.values():
            total_leafs += 1
    print(f"{total_leafs} leafs total")

    # Delete existing ARM graph
    cursor = graph.run("MATCH (q:Q_Document) DETACH DELETE(q)")
    cursor = graph.run("MATCH (q:Q_Entity) DETACH DELETE(q)")

    print("Building relationships...")

    # Add all the documents
    for i, d in enumerate(documents.values()):
        d_node = Node("Q_Document", **d)
        graph.merge(d_node, "Q_Document", "doc_id")
        print(f"\rDoc node {i} of {len(documents.values())}", end="")
    print()

    for i, e in enumerate(entity_parents.keys()):
        e_node = Node("Q_Entity", name=e, short_name=e.split("/")[-1])
        graph.merge(e_node, "Q_Entity", "name")
        print(f"\rEntity node {i} of {len(entity_parents.keys())}", end="")
    print()

    child_of = Relationship.type("CHILD_OF")
    for i, (e, p) in enumerate(entity_parents.items()):
        if p is None:
            continue
        e_node = Node("Q_Entity", name=e, short_name=e.split("/")[-1])
        p_node = Node("Q_Entity", name=p, short_name=p.split("/")[-1])
        graph.merge(child_of(e_node, p_node), "Q_Entity", "name")
        print(
            f"\rEntity relationship {i} of {len(entity_parents.items())}",
            end="",
        )

    print()
    print("Linking entities to documents...")

    # Link entities to the documents
    appears_in = Relationship.type("APPEARS_IN")
    for i, (e, data) in enumerate(leaf_entities.items()):
        for doc_id in data["doc_ids"]:
            graph.run(
                f"MATCH (d:Q_Document {{doc_id: {doc_id}}}),"
                f"(e: Q_Entity {{name: '{e}'}})"
                "MERGE (e)-[:APPEARS_IN]->(d)"
            )
        print(
            f"\rEntity to doc rel {i} of {len(leaf_entities.items())}", end=""
        )
    print()

    print("done")


def extract_transaction_data():
    """Extract the transaction data from the currently running Neo4j graph.

    Returns:
        transactions (list): A list of transaction data.
        documents (list): The list of documents.
        leaf_entities (dict): A dict mapping leaf entities to the docs in which
          they appear.
        entity_parents (dict): A dict mapping entities to their parents.
    """
    graph = Graph(password="password")
    tx = graph.begin()

    cursor = graph.run(
        "MATCH (i:Instance)-[a:APPEARS_IN]->(d:Document)"
        "RETURN d, d.doc_id as doc_id, d.tokens, collect(i.name) as entities,"
        "collect(labels(i)) as labels"
    )

    transactions = []

    documents = {}
    leaf_entities = {}
    entity_parents = {}  # A dict mapping entity to its parent

    records = []

    for i, record in enumerate(cursor):
        # if i > 100:
        #    break

        d = record["d"]
        doc_id = record["doc_id"]
        t_entities = record["entities"]
        t_labels = record["labels"]

        label_list = []

        for (entity, labels) in zip(t_entities, t_labels):
            if any(label in IGNORED_LABELS for label in labels):
                continue

            # Ignore the first label which is always Instance
            label_list.append(labels[1:])

        # Hacky implementation of multi-level rules
        # Construct a list of the expanded entities i.e.
        # item, item/rotating_equipment, item_rotating_equipment/pump, obs,
        # obs/leak etc.
        #
        # This allows the rules to be generated across different levels of
        # the concept hierarchy, but obviously results in many many rules
        # that need to be filtered later
        #
        all_labels = []
        for labels in label_list:

            for x, label in enumerate(labels):
                all_labels.append(label)

                # Build hierarchy
                if label not in entity_parents:
                    entity_parents[label] = None

                if x == 0:
                    continue

                entity_parents[label] = labels[x - 1]

        transactions.append(all_labels)

        documents[doc_id] = d
        for entity_labels in label_list:
            if entity_labels[-1] not in leaf_entities:
                leaf_entities[entity_labels[-1]] = {
                    "doc_ids": [],
                    "path": entity_labels,
                }
            leaf_entities[entity_labels[-1]]["doc_ids"].append(doc_id)

    with open("output/transactions.csv", "w") as f:
        for t in transactions:
            f.write(",".join(t))
            f.write("\n")

    return transactions, documents, leaf_entities, entity_parents


def run_arm(transactions):
    """Run apriori over the list of transactions.

    Args:
        transactions (list): The list of transaction data.

    Returns:
        list: A sorted list of rules.
    """
    print("Running ARM")
    itemsets, rules = apriori(
        transactions, min_support=0.0005, min_confidence=0.5
    )
    print("Done")

    print(f"{len(rules)} rules total")

    print("Filtering rules...")

    # Hacky implementation of multi-level ARM...
    # this simply filters out any rules containing more than one of the
    # same class of entity i.e. "Item"
    filtered_rules = []
    for i, rule in enumerate(rules):
        all_entities = []
        for e in rule.lhs:
            all_entities.append(e)
        for e in rule.rhs:
            all_entities.append(e)
        top_levels = set()
        good = True
        for e in all_entities:
            top_level = e.split("/")[0]
            if top_level in top_levels:
                good = False
            top_levels.add(top_level)
        if good:
            filtered_rules.append(rule)
        if i % 1000 == 0:
            print(f"Processed {i} rules")

    sorted_rules = sorted(filtered_rules, key=lambda x: x.lift, reverse=True)

    with open("output/rules.txt", "w") as f:
        for rule in sorted_rules:
            f.write(
                rule_to_string(rule) + f" (Lift: {'%.5f' % rule.lift}, "
                f"Conf: {'%.5f' % rule.confidence}, "
                f"Supp: {'%.5f' % rule.support})\n"
            )

    print(f"{len(filtered_rules)} rules total")
    return sorted_rules


def main():
    (
        transactions,
        documents,
        leaf_entities,
        entity_parents,
    ) = extract_transaction_data()
    build_transactions_graph(documents, leaf_entities, entity_parents)
    rules = run_arm(transactions)

    build_assocation_rule_graph(rules)


if __name__ == "__main__":
    main()
