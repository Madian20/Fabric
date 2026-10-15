[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared customers = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "9d196083-db64-4212-9c53-2f2ad21842f8"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "73781eba-72b7-4dad-9927-1179e19bc079"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "customers", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared order_items = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "9d196083-db64-4212-9c53-2f2ad21842f8"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "73781eba-72b7-4dad-9927-1179e19bc079"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "order_items", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared order_payments = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "9d196083-db64-4212-9c53-2f2ad21842f8"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "73781eba-72b7-4dad-9927-1179e19bc079"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "order_payments", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared order_reviews = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "9d196083-db64-4212-9c53-2f2ad21842f8"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "73781eba-72b7-4dad-9927-1179e19bc079"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "order_reviews", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared orders = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "9d196083-db64-4212-9c53-2f2ad21842f8"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "73781eba-72b7-4dad-9927-1179e19bc079"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "orders", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared product_category_name_translation = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "9d196083-db64-4212-9c53-2f2ad21842f8"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "73781eba-72b7-4dad-9927-1179e19bc079"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "product_category_name_translation", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared products = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "9d196083-db64-4212-9c53-2f2ad21842f8"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "73781eba-72b7-4dad-9927-1179e19bc079"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "products", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared sellers = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "9d196083-db64-4212-9c53-2f2ad21842f8"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "73781eba-72b7-4dad-9927-1179e19bc079"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "sellers", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
