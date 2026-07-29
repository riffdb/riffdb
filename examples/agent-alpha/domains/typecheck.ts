import {
  attachTag,
  createAuthor,
  createComment,
  createPost,
  createPostRoute,
  createSite,
  createTag,
  moderationQueue,
  postBySlug,
  postPage,
  publicFeed,
} from "./agent-blog/generated/typescript/client.js";
import {
  addOrderLine,
  createCustomer,
  createInventory,
  createOrder,
  createProduct,
  createStore,
  customerHistory,
  inventoryDashboard,
  openOrders,
  orderPage,
  reserveInventory,
} from "./agent-orders/generated/typescript/client.js";

const siteId = "018f0f8b-7c6d-7e31-8a4f-2c2d37a52b20";
const authorId = "018f0f8b-7c6d-7e31-8a4f-2c2d37a52b21";
const postId = "018f0f8b-7c6d-7e31-8a4f-2c2d37a52b22";
const commentId = "018f0f8b-7c6d-7e31-8a4f-2c2d37a52b23";
const tagId = "018f0f8b-7c6d-7e31-8a4f-2c2d37a52b24";
const storeId = "018f0f8b-7c6d-7e31-8a4f-2c2d37a52b30";
const customerId = "018f0f8b-7c6d-7e31-8a4f-2c2d37a52b31";
const productId = "018f0f8b-7c6d-7e31-8a4f-2c2d37a52b32";
const orderId = "018f0f8b-7c6d-7e31-8a4f-2c2d37a52b33";

export const blogWorkload = [
  moderationQueue({ site_id: siteId, status: "Pending", limit: 25 }),
  postBySlug({ site_id: siteId, slug: "safe-application-data" }),
  postPage({ site_id: siteId, post_id: postId, comments_after: null }),
  publicFeed({ site_id: siteId, status: "Published", limit: 20 }),
  createSite({ idempotency_key: "site", site_id: siteId, name: "Acme" }),
  createAuthor({
    idempotency_key: "author",
    site_id: siteId,
    author_id: authorId,
    display_name: "Morgan",
  }),
  createPost({
    idempotency_key: "post",
    site_id: siteId,
    post_id: postId,
    author_id: authorId,
    slug: "safe-application-data",
    title: "Safe data",
    body: "Bounded and symbolic.",
    status: "Published",
  }),
  createPostRoute({
    idempotency_key: "route",
    site_id: siteId,
    post_id: postId,
    slug: "safe-application-data",
  }),
  createComment({
    idempotency_key: "comment",
    site_id: siteId,
    comment_id: commentId,
    post_id: postId,
    author_id: authorId,
    body: "Looks good.",
    status: "Approved",
  }),
  createTag({ idempotency_key: "tag", site_id: siteId, tag_id: tagId, name: "safety" }),
  attachTag({
    idempotency_key: "attach",
    site_id: siteId,
    post_id: postId,
    tag_id: tagId,
  }),
];

export const ordersWorkload = [
  customerHistory({ store_id: storeId, customer_id: customerId, limit: 25 }),
  inventoryDashboard({ store_id: storeId, limit: 50 }),
  openOrders({ store_id: storeId, status: "Open", limit: 25 }),
  orderPage({ store_id: storeId, order_id: orderId }),
  createStore({ idempotency_key: "store", store_id: storeId, name: "Acme" }),
  createCustomer({
    idempotency_key: "customer",
    store_id: storeId,
    customer_id: customerId,
    display_name: "River",
  }),
  createProduct({
    idempotency_key: "product",
    store_id: storeId,
    product_id: productId,
    sku: "SAFE-001",
    name: "Safety Widget",
    unit_price: {
      coefficientTwosComplement: Uint8Array.from([0x07, 0x6c]),
      scale: 2,
      precision: 18,
    },
  }),
  createInventory({
    idempotency_key: "inventory",
    store_id: storeId,
    product_id: productId,
    available: 10n,
  }),
  createOrder({
    idempotency_key: "order",
    store_id: storeId,
    order_id: orderId,
    customer_id: customerId,
  }),
  addOrderLine({
    idempotency_key: "line",
    store_id: storeId,
    order_id: orderId,
    product_id: productId,
    quantity: 1n,
  }),
  reserveInventory({
    idempotency_key: "reserve",
    store_id: storeId,
    order_id: orderId,
    product_id: productId,
    quantity: 1n,
  }),
];
