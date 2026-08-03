const form = document.querySelector("#selection");
const connection = document.querySelector("#connection");
const queue = document.querySelector("#queue");
const ticket = document.querySelector("#ticket");
let streams = [];

form.addEventListener("submit", (event) => {
  event.preventDefault();
  for (const stream of streams) stream.close();
  const values = new FormData(form);
  const organization = String(values.get("organization_id"));
  const project = String(values.get("project_id"));
  const ticketId = String(values.get("ticket_id"));
  const queueStream = connect("/events/queue", { organization_id: organization, project_id: project }, queue);
  const ticketStream = connect("/events/ticket", { organization_id: organization, ticket_id: ticketId }, ticket);
  streams = [queueStream, ticketStream];
  connection.textContent = "Live";
});

function connect(path, parameters, target) {
  const url = new URL(path, window.location.href);
  for (const [name, value] of Object.entries(parameters)) url.searchParams.set(name, value);
  const source = new EventSource(url);
  for (const type of ["snapshot", "patch", "reset", "checkpoint"]) {
    source.addEventListener(type, (event) => render(target, JSON.parse(event.data)));
  }
  source.addEventListener("terminal", () => {
    target.textContent = "Authorization ended";
    target.className = "empty";
    source.close();
  });
  source.onerror = () => { connection.textContent = "Reconnecting"; };
  source.onopen = () => { connection.textContent = "Live"; };
  return source;
}

function render(target, value) {
  const output = document.createElement("pre");
  output.textContent = JSON.stringify(value, null, 2);
  target.className = "";
  target.replaceChildren(output);
}
