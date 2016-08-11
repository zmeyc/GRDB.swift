import Foundation

/// NSDate is stored in the database using the format
/// "yyyy-MM-dd HH:mm:ss.SSS", in the UTC time zone.
extension NSDate : DatabaseValueConvertible {
    
    /// Returns a value that can be stored in the database.
    public var databaseValue: DatabaseValue {
        #if os(Linux)
        return Date(timeIntervalSinceReferenceDate: self.timeIntervalSinceReferenceDate).databaseValue
        #else
        return (self as Date).databaseValue
        #endif
    }
    
    /// Returns an NSDate initialized from *databaseValue*, if possible.
    public static func fromDatabaseValue(_ databaseValue: DatabaseValue) -> Self? {
        if let date = Date.fromDatabaseValue(databaseValue) {
            #if os(Linux)
            // Error: constructing an object of class type 'Self' with a metatype value must use a 'required' initializer
            //return self.init(timeInterval: 0, sinceDate: date)
            // Workaround:
            let coder = NSCoder()
            let date = NSDate(timeInterval: 0, sinceDate: date)
            date.encode(with: coder)
            return self.init(coder: coder)
            #else
            return self.init(timeInterval: 0, since: date)
            #endif
        }
        return nil
    }
}
