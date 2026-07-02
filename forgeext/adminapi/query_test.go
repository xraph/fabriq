package adminapi

import (
	"net/http"
	"testing"
)

func TestPrecheckReadOnlySQL(t *testing.T) {
	ok := []string{
		"SELECT 1",
		"  select * from product ",
		"WITH x AS (SELECT 1) SELECT * FROM x",
		"select a from t where status = 'deleted'", // literal, not a write
		"select(1)",
		"with x as (select 1) select * from x",
	}
	for _, s := range ok {
		if err := precheckReadOnlySQL(s); err != nil {
			t.Errorf("precheck(%q) = %v, want nil", s, err)
		}
	}
	bad := []string{
		"DELETE FROM product",
		"update product set x = 1",
		"drop table product",
		"SELECT 1; DELETE FROM product", // statement stacking
		"",
		"selectfoo",
		"withdraw",
	}
	for _, s := range bad {
		if err := precheckReadOnlySQL(s); err == nil {
			t.Errorf("precheck(%q) = nil, want error", s)
		}
	}
}

func TestQueryRaw_501WithoutStores(t *testing.T) {
	// The fake-backed harness has no opened stores → the endpoint must 501.
	world := buildTestWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := doWrite(t, http.MethodPost, srv.URL+"/admin/query",
		map[string]any{"sql": "SELECT 1"})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotImplemented {
		t.Fatalf("status = %d, want 501", resp.StatusCode)
	}
}

func TestResolveEntityTables(t *testing.T) {
	// aliases maps entity name (singular), de-prefixed table (plural), and the
	// physical table — all to the physical table — the way handleRawQuery builds
	// it from the registry.
	aliases := map[string]string{
		"order": "ds_orders", "orders": "ds_orders", "ds_orders": "ds_orders",
		"customer": "ds_customers", "customers": "ds_customers", "ds_customers": "ds_customers",
		"product": "ds_products", "products": "ds_products", "ds_products": "ds_products",
	}
	cases := []struct{ name, in, want string }{
		{"de-prefixed table resolves", "SELECT * FROM customers", "SELECT * FROM ds_customers"},
		{"singular entity name resolves", "SELECT * FROM product", "SELECT * FROM ds_products"},
		{"reserved-word entity name resolves", "SELECT * FROM order o", "SELECT * FROM ds_orders o"},
		{"already physical unchanged", "SELECT * FROM ds_orders", "SELECT * FROM ds_orders"},
		{
			"mixed join — resolves bare names (entity or table), leaves columns alone",
			"SELECT o.id FROM ds_orders o JOIN customers c ON c.id = o.customer_id JOIN product p ON p.id = o.product_id ORDER BY o.id",
			"SELECT o.id FROM ds_orders o JOIN ds_customers c ON c.id = o.customer_id JOIN ds_products p ON p.id = o.product_id ORDER BY o.id",
		},
		{"unknown schema untouched", "SELECT table_name FROM information_schema.tables", "SELECT table_name FROM information_schema.tables"},
		{"unknown entity untouched", "SELECT * FROM widgets", "SELECT * FROM widgets"},
		{"lowercase from/join keywords", "select * from customers c join products p on p.id = c.pid", "select * from ds_customers c join ds_products p on p.id = c.pid"},
		{"string literal is not rewritten", "SELECT * FROM orders WHERE note = 'from products'", "SELECT * FROM ds_orders WHERE note = 'from products'"},
		{"'' escape inside literal preserved", "SELECT * FROM customers WHERE x = 'it''s from products'", "SELECT * FROM ds_customers WHERE x = 'it''s from products'"},
		{"line comment not rewritten", "SELECT * FROM orders -- join products here\nLIMIT 1", "SELECT * FROM ds_orders -- join products here\nLIMIT 1"},
		{"block comment not rewritten", "SELECT * FROM orders /* from products */ LIMIT 1", "SELECT * FROM ds_orders /* from products */ LIMIT 1"},
	}
	for _, tc := range cases {
		if got := resolveEntityTables(tc.in, aliases); got != tc.want {
			t.Errorf("%s:\n  in:   %s\n  got:  %s\n  want: %s", tc.name, tc.in, got, tc.want)
		}
	}
	// empty alias map is a no-op
	if got := resolveEntityTables("SELECT * FROM customers", nil); got != "SELECT * FROM customers" {
		t.Errorf("nil aliases should be a no-op, got %s", got)
	}
}
