package co.thebeat.bigdata.takehomeassignment.utils

import co.thebeat.bigdata.takehomeassignment.utils.Models.GeoPoint

object Polygon extends  App {

  val INF = Int.MaxValue

  // Given three colinear points p, q, r checks if point q lies on line segment 'pr'
  def onSegment(p:GeoPoint, q:GeoPoint, r:GeoPoint): Boolean = {
    if (q.lat < Math.max(p.lat, r.lat) && q.lat > Math.min(p.lat, r.lat) &&
      q.lng < Math.max(p.lng, r.lng) && q.lng > Math.min(p.lng, r.lng)
    )  true else false
  }

  // To find orientation of ordered triplet (p, q, r) returns following values.
  // 0 --> p, q and r are colinear
  // 1 --> Clockwise
  // 2 --> Counterclockwise
  def orientation(p:GeoPoint, q:GeoPoint, r:GeoPoint): Int = {
    val orient = (q.lng - p.lng) * (r.lat - q.lat) - (q.lat - p.lat) * (r.lng - q.lng);
    if (orient == 0) 0 else if (orient > 0) 1 else 2
  }

  // The function that returns true if line segment 'p1q1' and 'p2q2' intersect.
  def  doIntersect(p1:GeoPoint, q1:GeoPoint, p2:GeoPoint, q2:GeoPoint): Boolean = {

    val o1 = orientation(p1, q1, p2);
    val o2 = orientation(p1, q1, q2);
    val o3 = orientation(p2, q2, p1);
    val o4 = orientation(p2, q2, q1);

    // General case
    if (o1 != o2 && o3 != o4) return true

    // p1, q1 and p2 are colinear and p2 lies on segment p1q1
    if (o1 == 0 && onSegment(p1, p2, q1))  return true

    // p1, q1 and p2 are colinear and q2 lies on segment p1q1
    if (o2 == 0 && onSegment(p1, q2, q1))  return true;

    // p2, q2 and p1 are colinear and p1 lies on segment p2q2
    if (o3 == 0 && onSegment(p2, p1, q2))  return true

    // p2, q2 and q1 are colinear and q1 lies on segment p2q2
    if (o4 == 0 && onSegment(p2, q1, q2))  return true

    // Doesn't fall in any of the above cases
    false
  }

  // Returns true if the point p lies inside the polygon[] with n vertices
  def isInside(polygon:List[GeoPoint], p:GeoPoint): Boolean = {
    // There must be at least 3 vertices in polygon[]
    val n = polygon.length

    if (n < 3)  return false

    // Create a point for line segment from p to infinite
    val extreme = GeoPoint(INF, p.lng)

    // Count intersections of the above line with sides of polygon
    var count = 0
    var i = 0

    do {
      val next = (i + 1) % n;

      // Check if the line segment from 'p' to 'extreme' intersects with the line
      // segment from 'polygon[i]' to 'polygon[next]'
      if (doIntersect(polygon(i), polygon(next), p, extreme)) {

        // If the point 'p' is colinear with line segment 'i-next',
        // then check if it lies  on segment. If it lies, return true, otherwise false
        if (orientation(polygon(i), p, polygon(next)) == 0) return onSegment(polygon(i), p,  polygon(next))

        count = count + 1
      }
      i = next;
    } while (i != 0);

    // Return true if count is odd, false otherwise
    count % 2 == 1
  }


}
