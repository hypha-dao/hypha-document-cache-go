package dgraph

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/dgraph-io/dgo/v2"
	"github.com/dgraph-io/dgo/v2/protos/api"
	"google.golang.org/grpc"
)

//Dgraph abstracts dgraph Client
type Dgraph struct {
	Client *dgo.Dgraph
	Conn   *grpc.ClientConn
}

//New creates a new instance of DGraph
func New(addr string) (*Dgraph, error) {
	// Dial a gRPC connection. The address to dial to can be configured when
	// setting up the dgraph cluster.
	if addr == "" {
		addr = "localhost:9080"
	}
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	return &Dgraph{
		Client: dgo.NewDgraphClient(
			api.NewDgraphClient(conn),
		),
		Conn: conn,
	}, nil
}

//Alter Enables alter operations
func (m *Dgraph) Alter(op *api.Operation) error {
	return m.Client.Alter(context.Background(), op)
}

// UpdateSchema updates schema
func (m *Dgraph) UpdateSchema(schema string) error {
	return m.Alter(&api.Operation{
		Schema: schema,
	})
}

// Txn returns a new transaction
func (m *Dgraph) Txn(readOnly bool) *dgo.Txn {
	if readOnly {
		return m.Client.NewReadOnlyTxn()
	}
	return m.Client.NewTxn()
}

// Query executes a query
func (m *Dgraph) Query(query string, vars map[string]string, v interface{}) error {
	// log.Printf("query: %v, vars: %v", query, vars)
	txn := m.Txn(true)
	ctx := context.Background()
	defer txn.Discard(ctx)
	var (
		resp *api.Response
		err  error
	)
	if vars != nil {
		resp, err = txn.QueryWithVars(ctx, query, vars)
	} else {
		resp, err = txn.Query(ctx, query)
	}
	if err != nil {
		return err
	}
	// log.Printf("Query JSON response: %v", string(resp.GetJson()))
	if err := json.Unmarshal(resp.GetJson(), v); err != nil {
		return err
	}
	// log.Printf("Query Unmarshaled result: %v", v)
	return nil
}

//GetTypes Returns requested type definitions
func (m *Dgraph) GetTypes(typeNames []string) (*SchemaTypes, error) {
	schemaTypes := &SchemaTypes{}
	q := fmt.Sprintf(`schema(type:[%v]){}`, strings.Join(typeNames, ","))
	if err := m.Query(q, nil, schemaTypes); err != nil {
		return nil, err
	}
	return schemaTypes, nil
}

//GetType Returns requested type
func (m *Dgraph) GetType(typeName string) (*SchemaType, error) {
	schemaTypes, err := m.GetTypes([]string{typeName})
	if err != nil {
		return nil, err
	}
	if len(schemaTypes.Types) > 0 {
		return schemaTypes.Types[0], nil
	}
	return nil, nil
}

//MissingTypes Determines if the requested types exist, returns a slice with missing types
func (m *Dgraph) MissingTypes(typeNames []string) ([]string, error) {
	schemaTypes, err := m.GetTypes(typeNames)
	if err != nil {
		return nil, err
	}
	typeMap := make(map[string]bool, len(typeNames))
	for _, schemaType := range schemaTypes.Types {
		typeMap[schemaType.Name] = true
	}

	missing := make([]string, 0, len(typeNames))
	for _, typeName := range typeNames {
		if _, ok := typeMap[typeName]; !ok {
			missing = append(missing, typeName)
		}
	}
	return missing, nil
}

//GetTypeFieldMap Returns a map containing the fields of the type of the form fieldName -> SchemaField
func (m *Dgraph) GetTypeFieldMap(typeName string) (map[string]*SchemaField, error) {
	schemaType, err := m.GetType(typeName)
	if err != nil {
		return nil, err
	}
	fieldMap := make(map[string]*SchemaField, len(schemaType.Fields))
	for _, field := range schemaType.Fields {
		fieldMap[field.Name] = field
	}
	return fieldMap, nil
}

//MutateJSON Performs a json mutation
func (m *Dgraph) MutateJSON(v interface{}, deleteOp bool) (*api.Response, error) {
	vb, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return m.MutateJSONStr(vb, deleteOp)
}

//MutateJSONStr Performs a json mutation
func (m *Dgraph) MutateJSONStr(vb []byte, deleteOp bool) (*api.Response, error) {
	mutation := &api.Mutation{}
	if deleteOp {
		mutation.DeleteJson = vb
	} else {
		mutation.SetJson = vb
	}
	return m.MutateOne(mutation)
}

//MutateNQuads Performs a nquads mutation
func (m *Dgraph) MutateNQuads(v string, deleteOp bool) (*api.Response, error) {
	mutation := &api.Mutation{}
	vb := []byte(v)
	if deleteOp {
		mutation.DelNquads = vb
	} else {
		mutation.SetNquads = vb
	}
	return m.MutateOne(mutation)
}

//DeleteNQuads Performs a nquads delete
func (m *Dgraph) DeleteNQuads(v string) (*api.Response, error) {
	return m.MutateNQuads(v, true)
}

//DeleteNode Deletes a Node
func (m *Dgraph) DeleteNode(uid string) (*api.Response, error) {
	return m.DeleteNQuads(fmt.Sprintf("<%v> * * .", uid))
}

//MutateEdge Mutates an edge
func (m *Dgraph) MutateEdge(uidFrom, uidTo, edgeName string, deleteOp bool) (*api.Response, error) {
	return m.MutateNQuads(edgeTriplet(uidFrom, uidTo, edgeName), deleteOp)
}

func edgeTriplet(uidFrom, uidTo, edgeName string) string {
	return fmt.Sprintf("<%v> <%v> <%v> .", uidFrom, edgeName, uidTo)
}

//Mutate Performs multiple mutations
func (m *Dgraph) Mutate(mutations ...*api.Mutation) ([]*api.Response, error) {
	responses := make([]*api.Response, len(mutations), len(mutations))
	txn := m.Txn(false)
	ctx := context.Background()
	defer txn.Discard(ctx)
	for _, mutation := range mutations {
		response, err := txn.Mutate(ctx, mutation)
		if err != nil {
			return nil, err
		}
		responses = append(responses, response)
	}

	err := txn.Commit(ctx)
	if err != nil {
		return nil, err
	}
	return responses, nil
}

//MutateOne Performs a single mutation
func (m *Dgraph) MutateOne(mutation *api.Mutation) (*api.Response, error) {
	responses, err := m.Mutate(mutation)
	if err != nil {
		return nil, err
	}
	return responses[0], nil
}

//Drop performs a Drop Operation
func (m *Dgraph) Drop(dropOp api.Operation_DropOp) error {
	return m.Alter(&api.Operation{
		DropOp: dropOp,
	})
}

//DropData drops all data
func (m *Dgraph) DropData() error {
	return m.Drop(api.Operation_DATA)
}

//DropAll drops the database
func (m *Dgraph) DropAll() error {
	return m.Drop(api.Operation_ALL)
}

//Close closes connection
func (m *Dgraph) Close() error {
	return m.Conn.Close()
}
