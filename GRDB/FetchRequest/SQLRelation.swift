#if !USING_BUILTIN_SQLITE
    #if os(OSX)
        import SQLiteMacOSX
    #elseif os(iOS)
        #if (arch(i386) || arch(x86_64))
            import SQLiteiPhoneSimulator
        #else
            import SQLiteiPhoneOS
        #endif
    #endif
#endif

/// TODO: documentation
public protocol _SQLRelation {
    /// TODO: documentation
    @warn_unused_result
    func fork() -> Self
    
    /// TODO: documentation
    @warn_unused_result
    func numberOfColumns(db: Database) throws -> Int
    
    /// TODO: documentation
    var name: String { get }
    
    /// TODO: documentation
    var referencedSources: [_SQLSource] { get }
    
    /// TODO: documentation
    var rightSource: SQLSource { get }
    
    /// TODO: documentation
    func sql(db: Database, inout _ arguments: StatementArguments, leftSource: SQLSource, joinKind: _JoinKind, innerJoinForbidden: Bool) throws -> String
    
    /// TODO: documentation
    func selection(included included: Bool) -> [_SQLSelectable]
    
    /// TODO: documentation
    func adapter(included included: Bool, inout selectionIndex: Int, columnIndexForSelectionIndex: [Int: Int]) -> RowAdapter?
}

/// TODO: documentation
public protocol SQLRelation : _SQLRelation {
    /// TODO: documentation
    @warn_unused_result
    func aliased(alias: String) -> SQLRelation
    
    /// TODO: documentation
    @warn_unused_result
    func filter(predicate: (SQLSource) -> _SQLExpressible) -> SQLRelation
}

extension SQLRelation {
    
    /// TODO: documentation
    /// Extension method
    @warn_unused_result
    public func include(relations: SQLRelation...) -> SQLRelation {
        return include(required: false, relations)
    }
    
    /// TODO: documentation
    /// Extension method
    @warn_unused_result
    public func include(required required: Bool, _ relations: SQLRelation...) -> SQLRelation {
        return include(required: required, relations)
    }
    
    /// TODO: documentation
    /// Extension method
    @warn_unused_result
    public func include(relations: [SQLRelation]) -> SQLRelation {
        return include(required: false, relations)
    }
    
    /// TODO: documentation
    /// Extension method
    @warn_unused_result
    public func include(required required: Bool, _ relations: [SQLRelation]) -> SQLRelation {
        return ChainedRelation(baseRelation: self, joins: relations.map { Join(included: true, kind: required ? .Inner : .Left, relation: $0.fork()) })
    }
    
    /// TODO: documentation
    /// Extension method
    @warn_unused_result
    public func join(relations: SQLRelation...) -> SQLRelation {
        return join(required: false, relations)
    }
    
    /// TODO: documentation
    /// Extension method
    @warn_unused_result
    public func join(required required: Bool, _ relations: SQLRelation...) -> SQLRelation {
        return join(required: required, relations)
    }
    
    /// TODO: documentation
    /// Extension method
    @warn_unused_result
    public func join(relations: [SQLRelation]) -> SQLRelation {
        return join(required: false, relations)
    }
    
    /// TODO: documentation
    /// Extension method
    @warn_unused_result
    public func join(required required: Bool, _ relations: [SQLRelation]) -> SQLRelation {
        return ChainedRelation(baseRelation: self, joins: relations.map { Join(included: false, kind: required ? .Inner : .Left, relation: $0.fork()) })
    }
    
    /// TODO: documentation
    /// Extension method
    @warn_unused_result
    public func filter(sql sql: String, arguments: StatementArguments? = nil) -> SQLRelation {
        return filter { _ in _SQLExpression.Literal("(\(sql))", arguments) }
    }

}

/// TODO: documentation
public enum _JoinKind : String {
    case Inner = "JOIN"
    case Left = "LEFT JOIN"
    case Cross = "CROSS JOIN"
}

struct Join {
    let included: Bool
    let kind: _JoinKind
    let relation: SQLRelation
    
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
    let baseRelation: SQLRelation
    let joins: [Join]
}

extension ChainedRelation : _SQLRelation {
    /// TODO: documentation
    func fork() -> ChainedRelation {
        return ChainedRelation(baseRelation: baseRelation.fork(), joins: joins.map { $0.fork() })
    }
    
    /// TODO: documentation
    @warn_unused_result
    func numberOfColumns(db: Database) throws -> Int {
        return try joins.reduce(baseRelation.numberOfColumns(db)) { try $0 + $1.numberOfColumns(db) }
    }
    
    /// TODO: documentation
    var name: String {
        return baseRelation.name
    }
    
    /// TODO: documentation
    var referencedSources: [_SQLSource] {
        return joins.reduce(baseRelation.referencedSources) { $0 + $1.relation.referencedSources }
    }
    
    /// TODO: documentation
    var rightSource: SQLSource {
        return baseRelation.rightSource
    }
    
    /// TODO: documentation
    func sql(db: Database, inout _ arguments: StatementArguments, leftSource: SQLSource, joinKind: _JoinKind, innerJoinForbidden: Bool) throws -> String {
        var sql = try baseRelation.sql(db, &arguments, leftSource: leftSource, joinKind: joinKind, innerJoinForbidden: innerJoinForbidden)
        if !joins.isEmpty {
            let innerJoinForbidden = (joinKind == .Left)
            sql += " "
            sql += try joins.map {
                try $0.relation.sql(db, &arguments, leftSource: baseRelation.rightSource, joinKind: $0.kind, innerJoinForbidden: innerJoinForbidden)
                }.joinWithSeparator(" ")
        }
        return sql
    }
    
    /// TODO: documentation
    func selection(included included: Bool) -> [_SQLSelectable] {
        return joins.reduce(baseRelation.selection(included: included)) { (selection, join) in
            selection + join.relation.selection(included: join.included)
        }
    }
    
    /// TODO: documentation
    func adapter(included included: Bool, inout selectionIndex: Int, columnIndexForSelectionIndex: [Int: Int]) -> RowAdapter? {
        let baseAdapter = baseRelation.adapter(included: included, selectionIndex: &selectionIndex, columnIndexForSelectionIndex: columnIndexForSelectionIndex)
        
        var variants: [String: RowAdapter] = [:]
        for join in joins {
            if let adapter = join.relation.adapter(included: join.included, selectionIndex: &selectionIndex, columnIndexForSelectionIndex: columnIndexForSelectionIndex) {
                variants[join.relation.name] = adapter
            }
        }
        
        if baseAdapter == nil && variants.isEmpty {
            return nil
        }
        
        return (baseAdapter ?? ColumnMapping([:])).adapterWithVariants(variants)
    }
}

extension ChainedRelation : SQLRelation {
    
    /// TODO: documentation
    func aliased(alias: String) -> SQLRelation {
        return ChainedRelation(baseRelation: baseRelation.aliased(alias), joins: joins)
    }
    
    /// TODO: documentation
    func filter(predicate: (SQLSource) -> _SQLExpressible) -> SQLRelation {
        return ChainedRelation(baseRelation: baseRelation.filter(predicate), joins: joins)
    }
}

/// TODO: documentation
public struct ForeignRelation {
    /// TODO: documentation
    public let name: String
    /// TODO: documentation
    public let foreignKey: [String: String] // [leftColumn: rightColumn]
    /// TODO: documentation
    public private(set) var rightSource: SQLSource
    
    var predicate: ((left: SQLSource, right: SQLSource) -> _SQLExpressible)
    var selection: (SQLSource) -> [_SQLSelectable]
    
    /// TODO: documentation
    public init(named name: String? = nil, to tableName: String, through foreignKey: [String: String]) {
        // TODO: doesn't alias need to be validated as valid SQLite identifiers?
        let name = name ?? tableName
        let alias: String? = (name == tableName) ? nil : name
        let rightSource = _SQLSourceTable(tableName: tableName, alias: alias)
        self.init(name: name, rightSource: rightSource, foreignKey: foreignKey)
    }
    
    init(name: String, rightSource: SQLSource, foreignKey: [String: String]) {
        self.name = name
        self.rightSource = rightSource
        self.foreignKey = foreignKey
        self.predicate = { (left, right) in foreignKey.map { (leftColumn, rightColumn) in right[rightColumn] == left[leftColumn] }.reduce(&&) }
        self.selection = { (right) in [_SQLResultColumn.Star(right)] }
    }
}

extension ForeignRelation {
    /// TODO: documentation
    @warn_unused_result
    public func select(selection: (SQLSource) -> [_SQLSelectable]) -> ForeignRelation {
        var relation = self
        relation.selection = selection
        return relation
    }
}

extension ForeignRelation : _SQLRelation {
    /// TODO: documentation
    public func fork() -> ForeignRelation {
        var relation = self
        relation.rightSource = rightSource.fork()
        return relation
    }
    
    /// TODO: documentation
    @warn_unused_result
    public func numberOfColumns(db: Database) throws -> Int {
        return try selection(included: true).reduce(0) { (count, selectable) in try count + selectable.numberOfColumns(db) }
    }
    
    /// TODO: documentation
    public var referencedSources: [_SQLSource] {
        return rightSource.referencedSources
    }
    
    /// TODO: documentation
    public func sql(db: Database, inout _ arguments: StatementArguments, leftSource: SQLSource, joinKind: _JoinKind, innerJoinForbidden: Bool) throws -> String {
        if innerJoinForbidden && joinKind != .Left {
            throw DatabaseError(code: SQLITE_MISUSE, message: "Invalid required relation after a non-required relation.")
        }
        var sql = try joinKind.rawValue + " " + rightSource.sql(db, &arguments) + " ON "
        sql += try predicate(left: leftSource, right: rightSource).sqlExpression.sql(db, &arguments)
        return sql
    }
    
    /// TODO: documentation
    public func selection(included included: Bool) -> [_SQLSelectable] {
        return included ? selection(rightSource) : []
    }
    
    /// TODO: documentation
    public func adapter(included included: Bool, inout selectionIndex: Int, columnIndexForSelectionIndex: [Int: Int]) -> RowAdapter? {
        guard included else {
            return nil
        }
        defer { selectionIndex += 1 }
        return SuffixRowAdapter(fromIndex: columnIndexForSelectionIndex[selectionIndex]!)
    }
}

extension ForeignRelation : SQLRelation {
    /// TODO: documentation
    @warn_unused_result
    public func aliased(alias: String) -> SQLRelation {
        var relation = self
        relation.rightSource = rightSource.fork()
        relation.rightSource.name = alias
        return relation
    }
    
    /// TODO: documentation
    public func filter(predicate: (SQLSource) -> _SQLExpressible) -> SQLRelation {
        var relation = self
        let existingPredicate = self.predicate
        relation.predicate = { (left, right) in
            existingPredicate(left: left, right: right).sqlExpression && predicate(right).sqlExpression
        }
        return relation
    }
}

extension QueryInterfaceRequest {
    
    /// TODO: doc
    @warn_unused_result
    public func include(relations: SQLRelation...) -> QueryInterfaceRequest<T> {
        return include(required: false, relations)
    }
    
    /// TODO: doc
    @warn_unused_result
    public func include(required required: Bool, _ relations: SQLRelation...) -> QueryInterfaceRequest<T> {
        return include(required: required, relations)
    }
    
    /// TODO: doc
    @warn_unused_result
    public func include(relations: [SQLRelation]) -> QueryInterfaceRequest<T> {
        return include(required: false, relations)
    }
    
    /// TODO: doc
    /// TODO: test that request.include([assoc1, assoc2]) <=> request.include([assoc1]).include([assoc2])
    @warn_unused_result
    public func include(required required: Bool, _ relations: [SQLRelation]) -> QueryInterfaceRequest<T> {
        var query = self.query
        var source = query.source!
        for relation in relations {
            var relation = relation
            source = source.include(required: required, relation: &relation)
            query.joinedSelection.appendContentsOf(relation.selection(included: true))
        }
        query.source = source
        return QueryInterfaceRequest(query: query)
    }
    
    /// TODO: doc
    @warn_unused_result
    public func join(relations: SQLRelation...) -> QueryInterfaceRequest<T> {
        return join(required: false, relations)
    }
    
    /// TODO: doc
    @warn_unused_result
    public func join(required required: Bool, _ relations: SQLRelation...) -> QueryInterfaceRequest<T> {
        return join(required: required, relations)
    }
    
    /// TODO: doc
    @warn_unused_result
    public func join(relations: [SQLRelation]) -> QueryInterfaceRequest<T> {
        return join(required: false, relations)
    }
    
    /// TODO: doc
    /// TODO: test that request.join([assoc1, assoc2]) <=> request.join([assoc1]).join([assoc2])
    @warn_unused_result
    public func join(required required: Bool, _ relations: [SQLRelation]) -> QueryInterfaceRequest<T> {
        var query = self.query
        var source = query.source!
        for relation in relations {
            var relation = relation
            source = source.join(required: required, relation: &relation)
            query.joinedSelection.appendContentsOf(relation.selection(included: false))
        }
        query.source = source
        return QueryInterfaceRequest(query: query)
    }
}

extension TableMapping {
    /// TODO: doc
    @warn_unused_result
    public static func include(relations: SQLRelation...) -> QueryInterfaceRequest<Self> {
        return include(required: false, relations)
    }
    
    /// TODO: doc
    @warn_unused_result
    public static func include(required required: Bool, _ relations: SQLRelation...) -> QueryInterfaceRequest<Self> {
        return all().include(required: required, relations)
    }
    
    /// TODO: doc
    @warn_unused_result
    public static func include(relations: [SQLRelation]) -> QueryInterfaceRequest<Self> {
        return include(required: false, relations)
    }
    
    /// TODO: doc
    @warn_unused_result
    public static func include(required required: Bool, _ relations: [SQLRelation]) -> QueryInterfaceRequest<Self> {
        return all().include(required: required, relations)
    }
    
    /// TODO: doc
    @warn_unused_result
    public static func join(relations: SQLRelation...) -> QueryInterfaceRequest<Self> {
        return join(required: false, relations)
    }
    
    /// TODO: doc
    @warn_unused_result
    public static func join(required required: Bool, _ relations: SQLRelation...) -> QueryInterfaceRequest<Self> {
        return all().join(required: required, relations)
    }
    
    /// TODO: doc
    @warn_unused_result
    public static func join(relations: [SQLRelation]) -> QueryInterfaceRequest<Self> {
        return join(required: false, relations)
    }
    
    /// TODO: doc
    @warn_unused_result
    public static func join(required required: Bool, _ relations: [SQLRelation]) -> QueryInterfaceRequest<Self> {
        return all().join(required: required, relations)
    }
}
