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

/// TODO
public protocol _SQLRelation {
    /// TODO
    @warn_unused_result
    func fork() -> Self
    
    /// TODO
    @warn_unused_result
    func aliased(alias: String) -> SQLRelation
    
    /// TODO
    @warn_unused_result
    func numberOfColumns(db: Database) throws -> Int
    
    /// TODO
    var variantName: String { get }
    
    /// TODO
    var referencedSources: [_SQLSource] { get }
    
    /// TODO
    var rightSource: SQLSource { get }
    
    /// TODO
    func sql(db: Database, inout _ arguments: StatementArguments, leftSource: SQLSource, joinKind: _JoinKind, innerJoinForbidden: Bool) throws -> String
    
    /// TODO
    func selection(included included: Bool) -> [_SQLSelectable]
    
    /// TODO
    func adapter(included included: Bool, inout selectionIndex: Int, columnIndexForSelectionIndex: [Int: Int]) -> RowAdapter?
}

/// TODO
public protocol SQLRelation : _SQLRelation {
}

extension SQLRelation {
    
    /// TODO
    /// Extension method
    @warn_unused_result
    public func include(required required: Bool = false, _ relations: SQLRelation...) -> SQLRelation {
        return include(required: required, relations)
    }
    
    /// TODO
    /// Extension method
    @warn_unused_result
    public func include(required required: Bool = false, _ relations: [SQLRelation]) -> SQLRelation {
        return ChainedRelation(baseRelation: self, joins: relations.map { Join(included: true, kind: required ? .Inner : .Left, relation: $0.fork()) })
    }
    
    /// TODO
    /// Extension method
    @warn_unused_result
    public func join(required required: Bool = false, _ relations: SQLRelation...) -> SQLRelation {
        return join(required: required, relations)
    }
    
    /// TODO
    /// Extension method
    @warn_unused_result
    public func join(required required: Bool = false, _ relations: [SQLRelation]) -> SQLRelation {
        return ChainedRelation(baseRelation: self, joins: relations.map { Join(included: false, kind: required ? .Inner : .Left, relation: $0.fork()) })
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

extension ChainedRelation : SQLRelation {
    /// TODO
    func fork() -> ChainedRelation {
        return ChainedRelation(baseRelation: baseRelation.fork(), joins: joins.map { $0.fork() })
    }
    
    /// TODO
    func aliased(alias: String) -> SQLRelation {
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
    var rightSource: SQLSource {
        return baseRelation.rightSource
    }
    
    /// TODO
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
    public private(set) var rightSource: SQLSource
    
    var predicate: ((left: SQLSource, right: SQLSource) -> _SQLExpressible)
    
    /// TODO
    public init(to tableName: String, through foreignKey: [String: String], variantName: String? = nil) {
        // TODO: doesn't alias need to be validated as valid SQLite identifiers?
        let variantName = variantName ?? tableName
        let alias: String? = (variantName == tableName) ? nil : variantName
        let rightSource = _SQLSourceTable(tableName: tableName, alias: alias)
        self.init(variantName: variantName, rightSource: rightSource, foreignKey: foreignKey)
    }
    
    /// TODO
    public func filter(predicate: (left: SQLSource, right: SQLSource) -> _SQLExpressible) -> ForeignRelation {
        var relation = self
        let existingPredicate = self.predicate
        relation.predicate = { (left, right) in
            existingPredicate(left: left, right: right).sqlExpression && predicate(left: left, right: right).sqlExpression
        }
        return relation
    }
    
    init(variantName: String, rightSource: SQLSource, foreignKey: [String: String]) {
        self.variantName = variantName
        self.rightSource = rightSource
        self.foreignKey = foreignKey
        self.predicate = { (left, right) in foreignKey.map { (leftColumn, rightColumn) in right[rightColumn] == left[leftColumn] }.reduce(&&) }
    }
}

extension ForeignRelation : SQLRelation {
    /// TODO
    public func fork() -> ForeignRelation {
        var relation = self
        relation.rightSource = rightSource.fork()
        return relation
    }
    
    /// TODO
    @warn_unused_result
    public func aliased(alias: String) -> SQLRelation {
        var relation = self
        relation.rightSource = rightSource.fork()
        relation.rightSource.name = alias
        return relation
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
    public func sql(db: Database, inout _ arguments: StatementArguments, leftSource: SQLSource, joinKind: _JoinKind, innerJoinForbidden: Bool) throws -> String {
        if innerJoinForbidden && joinKind != .Left {
            throw DatabaseError(code: SQLITE_MISUSE, message: "Invalid required relation after a non-required relation.")
        }
        var sql = try joinKind.rawValue + " " + rightSource.sql(db, &arguments) + " ON "
        sql += try predicate(left: leftSource, right: rightSource).sqlExpression.sql(db, &arguments)
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
    public func include(required required: Bool = false, _ relations: SQLRelation...) -> QueryInterfaceRequest<T> {
        return include(required: required, relations)
    }
    
    /// TODO: doc
    /// TODO: test that request.include([assoc1, assoc2]) <=> request.include([assoc1]).include([assoc2])
    @warn_unused_result
    public func include(required required: Bool = false, _ relations: [SQLRelation]) -> QueryInterfaceRequest<T> {
        var query = self.query
        var source = query.source!
        for relation in relations {
            var relation = relation
            source = source.include(required: required, relation: &relation)
            query.selection.appendContentsOf(relation.selection(included: true))
        }
        query.source = source
        return QueryInterfaceRequest(query: query)
    }
    
    /// TODO: doc
    @warn_unused_result
    public func join(required required: Bool = false, _ relations: SQLRelation...) -> QueryInterfaceRequest<T> {
        return join(required: required, relations)
    }
    
    /// TODO: doc
    /// TODO: test that request.join([assoc1, assoc2]) <=> request.join([assoc1]).join([assoc2])
    @warn_unused_result
    public func join(required required: Bool = false, _ relations: [SQLRelation]) -> QueryInterfaceRequest<T> {
        var query = self.query
        var source = query.source!
        for relation in relations {
            var relation = relation
            source = source.join(required: required, relation: &relation)
            query.selection.appendContentsOf(relation.selection(included: false))
        }
        query.source = source
        return QueryInterfaceRequest(query: query)
    }
}

extension TableMapping {
    /// TODO: doc
    @warn_unused_result
    public static func include(required required: Bool = false, _ relations: SQLRelation...) -> QueryInterfaceRequest<Self> {
        return all().include(required: required, relations)
    }
    
    /// TODO: doc
    @warn_unused_result
    public static func include(required required: Bool = false, _ relations: [SQLRelation]) -> QueryInterfaceRequest<Self> {
        return all().include(required: required, relations)
    }
    
    /// TODO: doc
    @warn_unused_result
    public static func join(required required: Bool = false, _ relations: SQLRelation...) -> QueryInterfaceRequest<Self> {
        return all().join(required: required, relations)
    }
    
    /// TODO: doc
    @warn_unused_result
    public static func join(required required: Bool = false, _ relations: [SQLRelation]) -> QueryInterfaceRequest<Self> {
        return all().join(required: required, relations)
    }
}
