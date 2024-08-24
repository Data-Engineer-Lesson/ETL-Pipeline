def transform_data(customers, orders, products, order_details, payments, loyalty_programs):
    # order_details tablosuna ürün fiyatını ekle ve price sütununu seç
    order_details = order_details.merge(products[['product_id', 'price']], on='product_id', how='left')

    # price sütununu düzeltmek için 'price_y' olarak kullanın
    order_details['price'] = order_details['price_y']  # 'price_y' products tablosundan geliyor olabilir

    # Artık price_x ve price_y'yi silebiliriz
    order_details.drop(columns=['price_x', 'price_y'], inplace=True)

    print("order_details after merge and fixing price:")
    print(order_details.head())  # price sütununun doğru olduğundan emin olun

    # Müşterinin yaptığı toplam harcama ve sipariş sayısını hesapla
    customer_order_summary = orders.merge(order_details, on='order_id').groupby('customer_id').agg({
        'order_id': 'count',
        'price': 'sum'
    }).rename(columns={'order_id': 'order_count', 'price': 'total_spent'}).reset_index()

    # Ürün kategorileri bazında toplam satış ve adet bilgilerini hesapla
    # products tablosuyla merge yaparken fiyat sütunu zaten mevcutsa price_y oluşabilir, bu yüzden price_y'yi alacağız
    category_sales = order_details.merge(products[['product_id', 'category']], on='product_id', how='left').groupby('category').agg({
        'quantity': 'sum',
        'price': 'sum'
    }).rename(columns={'quantity': 'total_quantity', 'price': 'total_sales'}).reset_index()

    print("category_sales:")
    print(category_sales.head())

    # Müşteri sadakat programı verileri ile toplam harcamaları birleştir
    customer_loyalty = customer_order_summary.merge(loyalty_programs, on='customer_id', how='left')

    # Müşteri verilerine sadakat programı bilgilerini ve sipariş özetini ekle
    final_customer_data = customers.merge(customer_loyalty, on='customer_id', how='left')

    # Her bir ödeme yöntemi için toplam ödemeyi hesapla
    payment_summary = payments.groupby('payment_method').agg({
        'amount': 'sum'
    }).rename(columns={'amount': 'total_amount'}).reset_index()

    return final_customer_data, category_sales, payment_summary
    
   