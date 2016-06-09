/// TODO
public protocol _Association {
    /// TODO
    @warn_unused_result
    func fork() -> Self
    
    /// TODO
    @warn_unused_result
    func aliased(alias: String) -> Association
    
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
public protocol Association : _Association {
}

extension Association {
    /// TODO
    /// extension Method
    @warn_unused_result
    public func include(associations: Association...) -> Association {
        return include(associations)
    }
    
    /// TODO
    /// extension Method
    @warn_unused_result
    public func include(associations: [Association]) -> Association {
        return ChainedAssociation(baseAssociation: self, rightAssociations: associations.map { $0.fork() })
    }
}

struct ChainedAssociation {
    let baseAssociation: Association
    let rightAssociations: [Association]
}

extension ChainedAssociation : Association {
    /// TODO
    func fork() -> ChainedAssociation {
        return ChainedAssociation(baseAssociation: baseAssociation.fork(), rightAssociations: rightAssociations.map { $0.fork() })
    }
    
    /// TODO
    func aliased(alias: String) -> Association {
        return ChainedAssociation(baseAssociation: baseAssociation.aliased(alias), rightAssociations: rightAssociations)
    }
    
    /// TODO
    @warn_unused_result
    func numberOfColumns(db: Database) throws -> Int {
        return try rightAssociations.reduce(baseAssociation.numberOfColumns(db)) { try $0 + $1.numberOfColumns(db) }
    }
    
    /// TODO
    var name: String {
        return baseAssociation.name
    }
    
    /// TODO
    var referencedSources: [_SQLSource] {
        return rightAssociations.reduce(baseAssociation.referencedSources) { $0 + $1.referencedSources }
    }
    
    /// TODO
    var rightSource: _SQLSource {
        return baseAssociation.rightSource
    }
    
    /// TODO
    var selection: [_SQLSelectable] {
        return rightAssociations.reduce(baseAssociation.selection) { (selection, association) in
            selection + association.selection
        }
    }
    
    /// TODO
    func sql(db: Database, inout _ bindings: [DatabaseValueConvertible?], leftSourceName: String) throws -> String {
        var sql = try baseAssociation.sql(db, &bindings, leftSourceName: leftSourceName)
        if !rightAssociations.isEmpty {
            sql += " "
            sql += try rightAssociations.map {
                try $0.sql(db, &bindings, leftSourceName: baseAssociation.rightSource.name!)
                }.joinWithSeparator(" ")
        }
        return sql
    }
    
    /// TODO
    func adapter(inout selectionIndex: Int, columnIndexForSelectionIndex: [Int: Int]) -> RowAdapter {
        let adapter = baseAssociation.adapter(&selectionIndex, columnIndexForSelectionIndex: columnIndexForSelectionIndex)
        var variants: [String: RowAdapter] = [:]
        for association in rightAssociations {
            variants[association.name] = association.adapter(&selectionIndex, columnIndexForSelectionIndex: columnIndexForSelectionIndex)
        }
        return adapter.adapterWithVariants(variants)
    }
}

/// TODO
public struct Join {
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

extension Join : Association {
    /// TODO
    public func fork() -> Join {
        return Join(name: name, rightSource: rightSource.copy(), foreignKey: foreignKey)
    }
    
    /// TODO
    @warn_unused_result
    public func aliased(alias: String) -> Association {
        let rightSource = self.rightSource.copy()
        rightSource.name = alias
        return Join(name: name, rightSource: rightSource, foreignKey: foreignKey)
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
    public func include(associations: Association...) -> QueryInterfaceRequest<T> {
        return include(associations)
    }
    
    /// TODO: doc
    /// TODO: test that request.include([assoc1, assoc2]) <=> request.include([assoc1]).include([assoc2])
    @warn_unused_result
    public func include(associations: [Association]) -> QueryInterfaceRequest<T> {
        var query = self.query
        var source = query.source!
        for association in associations {
            source = source.include(association)
            query.selection.appendContentsOf(association.selection)
        }
        query.source = source
        return QueryInterfaceRequest(query: query)
    }
}

extension TableMapping {
    /// TODO: doc
    @warn_unused_result
    public static func include(associations: Association...) -> QueryInterfaceRequest<Self> {
        return all().include(associations)
    }
    
    /// TODO: doc
    @warn_unused_result
    public static func include(associations: [Association]) -> QueryInterfaceRequest<Self> {
        return all().include(associations)
    }
}
