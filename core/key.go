package core

var (
	// PrefixLink prefix for a link declaration.
	PrefixLink = "link%"
	// PrefixTable prefix for a table entry.
	PrefixTable = "tbl%"

	// Delimiter between the tableName and the key
	Delimiter     = "_"
	LinkDelimiter = "@"
)

func MakePrefix(tableName string) string {
	return PrefixTable + tableName + Delimiter
}

// TODO check if this function could be used more frequently.
func MakeKey(tableName, id string) []byte {
	return []byte(MakePrefix(tableName) + id)
}

func MakeLinkKey(tableName string, id string, targetName string, targetId string) []string {

	k1 := tableName + Delimiter + id
	k2 := targetName + Delimiter + targetId

	return []string{k1 + LinkDelimiter + k2, k2 + LinkDelimiter + k1}
}
