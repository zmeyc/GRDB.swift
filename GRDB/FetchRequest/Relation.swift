/// TODO
public protocol _Relation {
    /// TODO
    @warn_unused_result
    func fork() -> Self
    
    /// TODO
    @warn_unused_result
    func aliased(alias: String) -> Relation
    
    /// TODO
    @warn_unused_result
    func numberOfColumns(db: Database) throws -> Int
    
    /// TODO
    var variantName: String { get }
    
    /// TODO
    var referencedSources: [_SQLSource] { get }
    
    /// TODO
    var rightSource: _SQLSource { get }
    
    /// TODO
    func sql(db: Database, inout _ bindings: [DatabaseValueConvertible?], joinKind: _JoinKind, leftSourceName: String) throws -> String
    
    /// TODO
    func selection(included included: Bool) -> [_SQLSelectable]
    
    /// TODO
    func adapter(included included: Bool, inout selectionIndex: Int, columnIndexForSelectionIndex: [Int: Int]) -> RowAdapter?
}

/// TODO
public protocol Relation : _Relation {
}

extension Relation {
    /// TODO
    /// extension Method
    @warn_unused_result
    public func include(relations: Relation...) -> Relation {
        return include(relations)
    }
    
    /// TODO
    /// extension Method
    @warn_unused_result
    public func include(relations: [Relation]) -> Relation {
        return ChainedRelation(baseRelation: self, joins: relations.map { Join(included: true, kind: .Left, relation: $0.fork()) })
    }
    
    /// TODO
    /// extension Method
    @warn_unused_result
    public func join(relations: Relation...) -> Relation {
        return join(relations)
    }
    
    /// TODO
    /// extension Method
    @warn_unused_result
    public func join(relations: [Relation]) -> Relation {
        return ChainedRelation(baseRelation: self, joins: relations.map { Join(included: false, kind: .Left, relation: $0.fork()) })
    }
}

/// TODO
public enum _JoinKind : String {
    case Inner = "JOIN"
    case Left = "LEFT JOIN"
    case Cross = "CROSS JOIN"
}

struct Join {
    let included: Bool
    let kind: _JoinKind
    let relation: Relation
    
    func fork() -> Join {
        return Join(included: included, kind: kind, relation: relation.fork())
    }
    
    func numberOfColumns(db: Database) throws -> Int {
        if included {
            return try relation.numberOfColumns(db)
        } else {
            return 0
        }
    }
}

struct ChainedRelation {
    let baseRelation: Relation
    let joins: [Join]
}

extension ChainedRelation : Relation {
    /// TODO
    func fork() -> ChainedRelation {
        return ChainedRelation(baseRelation: baseRelation.fork(), joins: joins.map { $0.fork() })
    }
    
    /// TODO
    func aliased(alias: String) -> Relation {
        return ChainedRelation(baseRelation: baseRelation.aliased(alias), joins: joins)
    }
    
    /// TODO
    @warn_unused_result
    func numberOfColumns(db: Database) throws -> Int {
        return try joins.reduce(baseRelation.numberOfColumns(db)) { try $0 + $1.numberOfColumns(db) }
    }
    
    /// TODO
    var variantName: String {
        return baseRelation.variantName
    }
    
    /// TODO
    var referencedSources: [_SQLSource] {
        return joins.reduce(baseRelation.referencedSources) { $0 + $1.relation.referencedSources }
    }
    
    /// TODO
    var rightSource: _SQLSource {
        return baseRelation.rightSource
    }
    
    /// TODO
    func sql(db: Database, inout _ bindings: [DatabaseValueConvertible?], joinKind: _JoinKind, leftSourceName: String) throws -> String {
        var sql = try baseRelation.sql(db, &bindings, joinKind: joinKind, leftSourceName: leftSourceName)
        if !joins.isEmpty {
            sql += " "
            sql += try joins.map {
                try $0.relation.sql(db, &bindings, joinKind: $0.kind, leftSourceName: baseRelation.rightSource.name!)
                }.joinWithSeparator(" ")
        }
        return sql
    }
    
    /// TODO
    func selection(included included: Bool) -> [_SQLSelectable] {
        return joins.reduce(baseRelation.selection(included: included)) { (selection, join) in
            selection + join.relation.selection(included: join.included)
        }
    }
    
    /// TODO
    func adapter(included included: Bool, inout selectionIndex: Int, columnIndexForSelectionIndex: [Int: Int]) -> RowAdapter? {
        let baseAdapter = baseRelation.adapter(included: included, selectionIndex: &selectionIndex, columnIndexForSelectionIndex: columnIndexForSelectionIndex)
        
        var variants: [String: RowAdapter] = [:]
        for join in joins {
            if let adapter = join.relation.adapter(included: join.included, selectionIndex: &selectionIndex, columnIndexForSelectionIndex: columnIndexForSelectionIndex) {
                variants[join.relation.variantName] = adapter
            }
        }
        
        if baseAdapter == nil && variants.isEmpty {
            return nil
        }
        
        return (baseAdapter ?? ColumnMapping([:])).adapterWithVariants(variants)
    }
}

/// TODO
public struct ForeignRelation {
    /// TODO
    public let variantName: String
    /// TODO
    public let foreignKey: [String: String] // [leftColumn: rightColumn]
    /// TODO
    public let rightSource: _SQLSource
    
    /// TODO
    public init(variantName: String? = nil, tableName: String, foreignKey: [String: String]) {
        // TODO: doesn't alias need to be validated as valid SQLite identifiers?
        let variantName = variantName ?? tableName
        let alias: String? = (variantName == tableName) ? nil : variantName
        let rightSource = _SQLSourceTable(tableName: tableName, alias: alias)
        self.init(variantName: variantName, rightSource: rightSource, foreignKey: foreignKey)
    }
    
    init(variantName: String, rightSource: _SQLSource, foreignKey: [String: String]) {
        self.variantName = variantName
        self.rightSource = rightSource
        self.foreignKey = foreignKey
    }
}

extension ForeignRelation : Relation {
    /// TODO
    public func fork() -> ForeignRelation {
        return ForeignRelation(variantName: variantName, rightSource: rightSource.fork(), foreignKey: foreignKey)
    }
    
    /// TODO
    @warn_unused_result
    public func aliased(alias: String) -> Relation {
        let rightSource = self.rightSource.fork()
        rightSource.name = alias
        return ForeignRelation(variantName: variantName, rightSource: rightSource, foreignKey: foreignKey)
    }
    
    /// TODO
    @warn_unused_result
    public func numberOfColumns(db: Database) throws -> Int {
        return try rightSource.numberOfColumns(db)
    }
    
    /// TODO
    public var referencedSources: [_SQLSource] {
        return rightSource.referencedSources
    }
    
    /// TODO
    public func sql(db: Database, inout _ bindings: [DatabaseValueConvertible?], joinKind: _JoinKind, leftSourceName: String) throws -> String {
        var sql = try joinKind.rawValue + " " + rightSource.sql(db, &bindings) + " ON "
        sql += foreignKey.map({ (leftColumn, rightColumn) -> String in
            "\(rightSource.name!.quotedDatabaseIdentifier).\(rightColumn.quotedDatabaseIdentifier) = \(leftSourceName.quotedDatabaseIdentifier).\(leftColumn.quotedDatabaseIdentifier)"
        }).joinWithSeparator(" AND ")
        return sql
    }
    
    /// TODO
    public func selection(included included: Bool) -> [_SQLSelectable] {
        guard included else {
            return []
        }
        return [_SQLResultColumn.Star(rightSource)]
    }
    
    /// TODO
    public func adapter(included included: Bool, inout selectionIndex: Int, columnIndexForSelectionIndex: [Int: Int]) -> RowAdapter? {
        guard included else {
            return nil
        }
        defer { selectionIndex += 1 }
        return SuffixRowAdapter(fromIndex: columnIndexForSelectionIndex[selectionIndex]!)
    }
}

extension QueryInterfaceRequest {
    /// TODO: doc
    @warn_unused_result
    public func include(required required: Bool = false, _ relations: Relation...) -> QueryInterfaceRequest<T> {
        return include(required: required, relations)
    }
    
    /// TODO: doc
    /// TODO: test that request.include([assoc1, assoc2]) <=> request.include([assoc1]).include([assoc2])
    @warn_unused_result
    public func include(required required: Bool = false, _ relations: [Relation]) -> QueryInterfaceRequest<T> {
        var query = self.query
        var source = query.source!
        for relation in relations {
            source = source.include(required: required, relation: relation)
            query.selection.appendContentsOf(relation.selection(included: true))
        }
        query.source = source
        return QueryInterfaceRequest(query: query)
    }
    
    /// TODO: doc
    @warn_unused_result
    public func join(required required: Bool = false, _ relations: Relation...) -> QueryInterfaceRequest<T> {
        return join(required: required, relations)
    }
    
    /// TODO: doc
    /// TODO: test that request.join([assoc1, assoc2]) <=> request.join([assoc1]).join([assoc2])
    @warn_unused_result
    public func join(required required: Bool = false, _ relations: [Relation]) -> QueryInterfaceRequest<T> {
        var query = self.query
        var source = query.source!
        for relation in relations {
            source = source.join(required: required, relation: relation)
            query.selection.appendContentsOf(relation.selection(included: false))
        }
        query.source = source
        return QueryInterfaceRequest(query: query)
    }
}

extension TableMapping {
    /// TODO: doc
    @warn_unused_result
    public static func include(required required: Bool = false, _ relations: Relation...) -> QueryInterfaceRequest<Self> {
        return all().include(required: required, relations)
    }
    
    /// TODO: doc
    @warn_unused_result
    public static func include(required required: Bool = false, _ relations: [Relation]) -> QueryInterfaceRequest<Self> {
        return all().include(required: required, relations)
    }
    
    /// TODO: doc
    @warn_unused_result
    public static func join(required required: Bool = false, _ relations: Relation...) -> QueryInterfaceRequest<Self> {
        return all().join(required: required, relations)
    }
    
    /// TODO: doc
    @warn_unused_result
    public static func join(required required: Bool = false, _ relations: [Relation]) -> QueryInterfaceRequest<Self> {
        return all().join(required: required, relations)
    }
}
