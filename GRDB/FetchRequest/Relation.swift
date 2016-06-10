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
    var name: String { get }
    
    /// TODO
    var referencedSources: [_SQLSource] { get }
    
    /// TODO
    var rightSource: _SQLSource { get }
    
    /// TODO
    var selection: [_SQLSelectable] { get }
    
    /// TODO
    func sql(db: Database, inout _ bindings: [DatabaseValueConvertible?], leftSourceName: String) throws -> String
    
    /// TODO
    func adapter(inout selectionIndex: Int, columnIndexForSelectionIndex: [Int: Int]) -> RowAdapter
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
        return ChainedRelation(baseRelation: self, rightRelations: relations.map { $0.fork() })
    }
}

struct ChainedRelation {
    let baseRelation: Relation
    let rightRelations: [Relation]
}

extension ChainedRelation : Relation {
    /// TODO
    func fork() -> ChainedRelation {
        return ChainedRelation(baseRelation: baseRelation.fork(), rightRelations: rightRelations.map { $0.fork() })
    }
    
    /// TODO
    func aliased(alias: String) -> Relation {
        return ChainedRelation(baseRelation: baseRelation.aliased(alias), rightRelations: rightRelations)
    }
    
    /// TODO
    @warn_unused_result
    func numberOfColumns(db: Database) throws -> Int {
        return try rightRelations.reduce(baseRelation.numberOfColumns(db)) { try $0 + $1.numberOfColumns(db) }
    }
    
    /// TODO
    var name: String {
        return baseRelation.name
    }
    
    /// TODO
    var referencedSources: [_SQLSource] {
        return rightRelations.reduce(baseRelation.referencedSources) { $0 + $1.referencedSources }
    }
    
    /// TODO
    var rightSource: _SQLSource {
        return baseRelation.rightSource
    }
    
    /// TODO
    var selection: [_SQLSelectable] {
        return rightRelations.reduce(baseRelation.selection) { (selection, relation) in
            selection + relation.selection
        }
    }
    
    /// TODO
    func sql(db: Database, inout _ bindings: [DatabaseValueConvertible?], leftSourceName: String) throws -> String {
        var sql = try baseRelation.sql(db, &bindings, leftSourceName: leftSourceName)
        if !rightRelations.isEmpty {
            sql += " "
            sql += try rightRelations.map {
                try $0.sql(db, &bindings, leftSourceName: baseRelation.rightSource.name!)
                }.joinWithSeparator(" ")
        }
        return sql
    }
    
    /// TODO
    func adapter(inout selectionIndex: Int, columnIndexForSelectionIndex: [Int: Int]) -> RowAdapter {
        let adapter = baseRelation.adapter(&selectionIndex, columnIndexForSelectionIndex: columnIndexForSelectionIndex)
        var variants: [String: RowAdapter] = [:]
        for relation in rightRelations {
            variants[relation.name] = relation.adapter(&selectionIndex, columnIndexForSelectionIndex: columnIndexForSelectionIndex)
        }
        return adapter.adapterWithVariants(variants)
    }
}

/// TODO
public struct ForeignRelation {
    /// TODO
    public let name: String
    /// TODO
    public let foreignKey: [String: String] // [leftColumn: rightColumn]
    /// TODO
    public let rightSource: _SQLSource
    
    /// TODO
    public init(name: String, tableName: String, foreignKey: [String: String]) {
        // TODO: why this forced alias?
        self.init(name: name, rightSource: _SQLSourceTable(tableName: tableName, alias: ((name == tableName) ? nil : name)), foreignKey: foreignKey)
    }
    
    init(name: String, rightSource: _SQLSource, foreignKey: [String: String]) {
        self.name = name
        self.rightSource = rightSource
        self.foreignKey = foreignKey
    }
}

extension ForeignRelation : Relation {
    /// TODO
    public func fork() -> ForeignRelation {
        return ForeignRelation(name: name, rightSource: rightSource.fork(), foreignKey: foreignKey)
    }
    
    /// TODO
    @warn_unused_result
    public func aliased(alias: String) -> Relation {
        let rightSource = self.rightSource.fork()
        rightSource.name = alias
        return ForeignRelation(name: name, rightSource: rightSource, foreignKey: foreignKey)
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
    public var selection: [_SQLSelectable] {
        return [_SQLResultColumn.Star(rightSource)]
    }
    
    /// TODO
    public func sql(db: Database, inout _ bindings: [DatabaseValueConvertible?], leftSourceName: String) throws -> String {
        var sql = try "LEFT JOIN " + rightSource.sql(db, &bindings) + " ON "
        sql += foreignKey.map({ (leftColumn, rightColumn) -> String in
            "\(rightSource.name!.quotedDatabaseIdentifier).\(rightColumn.quotedDatabaseIdentifier) = \(leftSourceName.quotedDatabaseIdentifier).\(leftColumn.quotedDatabaseIdentifier)"
        }).joinWithSeparator(" AND ")
        return sql
    }
    
    /// TODO
    public func adapter(inout selectionIndex: Int, columnIndexForSelectionIndex: [Int: Int]) -> RowAdapter {
        defer { selectionIndex += 1 }
        return SuffixRowAdapter(fromIndex: columnIndexForSelectionIndex[selectionIndex]!)
    }
}

extension QueryInterfaceRequest {
    /// TODO: doc
    @warn_unused_result
    public func include(relations: Relation...) -> QueryInterfaceRequest<T> {
        return include(relations)
    }
    
    /// TODO: doc
    /// TODO: test that request.include([assoc1, assoc2]) <=> request.include([assoc1]).include([assoc2])
    @warn_unused_result
    public func include(relations: [Relation]) -> QueryInterfaceRequest<T> {
        var query = self.query
        var source = query.source!
        for relation in relations {
            source = source.include(relation)
            query.selection.appendContentsOf(relation.selection)
        }
        query.source = source
        return QueryInterfaceRequest(query: query)
    }
}

extension TableMapping {
    /// TODO: doc
    @warn_unused_result
    public static func include(relations: Relation...) -> QueryInterfaceRequest<Self> {
        return all().include(relations)
    }
    
    /// TODO: doc
    @warn_unused_result
    public static func include(relations: [Relation]) -> QueryInterfaceRequest<Self> {
        return all().include(relations)
    }
}
